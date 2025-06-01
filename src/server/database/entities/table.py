import os
import io
import csv
import json
import mmap
from typing import Any, Callable
from server.config.settings import STORAGE_FOLDER
from server.utils.query_utils import ExpressionNode

# =========================================
# Hàm cast module-level để tránh lỗi pickle hoặc exec lặp
# =========================================

def cast_int(raw: str) -> int:
    return int(raw) if raw != "" else 0

def cast_float(raw: str) -> float:
    return float(raw) if raw != "" else 0.0

def cast_string(raw: str) -> str:
    return raw.strip()

class MMapReader(io.RawIOBase):
    """
    Wrapper cho mmap.mmap để cung cấp interface cần thiết (readable, read, readline, seek, tell),
    giúp io.TextIOWrapper có thể bọc và tạo thành file-like object (text stream) cho csv.reader.
    """
    def __init__(self, mm: mmap.mmap):
        self.mm = mm

    def readable(self) -> bool:
        return True

    def read(self, size: int = -1) -> bytes:
        return self.mm.read(size)

    def readline(self, size: int = -1) -> bytes:
        return self.mm.readline(size)

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        return self.mm.seek(offset, whence)

    def tell(self) -> int:
        return self.mm.tell()

    def close(self) -> None:
        # Khi đóng wrapper, chúng ta không đóng mmap ngay—để bên ngoài chủ động đóng mmap.
        super().close()

class Table:
    def __init__(self, table_name: str, db_name: str, columns_metadata: list[dict[str, Any]]):
        self.name = table_name
        self.csv_path = os.path.join(STORAGE_FOLDER, db_name, f"{table_name}.csv")
        self.column_metadata = columns_metadata
        # Map tên cột -> kiểu ('integer','float','string')
        self.column_types = {meta["name"]: meta["type"] for meta in self.column_metadata}
        # Map kiểu -> hàm cast tương ứng
        self._type_to_cast_fn: dict[str, Callable[[str], Any]] = {
            "integer": cast_int,
            "float": cast_float,
            "string": cast_string
        }

    def _open_mmap(self) -> mmap.mmap:
        """
        Mở file CSV dưới dạng memory-mapped; trả về mmap object đọc-only.
        """
        fd = os.open(self.csv_path, os.O_RDONLY)
        # length=0 để ánh xạ toàn bộ file
        mm = mmap.mmap(fd, length=0, access=mmap.ACCESS_READ)
        os.close(fd)
        return mm

    def query(self, columns: list[str], ast: Any = None):
        """
        - columns: list tên cột user muốn SELECT (hoặc ["*"] để lấy tất cả).
        - ast: ExpressionNode (cây điều kiện WHERE). Nếu None, chọn tất cả hàng.

        Trả về một generator, mỗi yield là JSON string (đã lọc + cast).
        """

        # 1) Mở mmap và bọc thành TextIOWrapper để dùng csv.reader
        mm = self._open_mmap()
        mmap_reader = MMapReader(mm)
        text_stream = io.TextIOWrapper(mmap_reader, encoding="utf-8", newline="")
        reader = csv.reader(text_stream)

        # Đọc header
        try:
            headers = next(reader)
        except StopIteration:
            text_stream.close()
            mm.close()
            raise ValueError("CSV file is empty.")
        headers = [h.strip() for h in headers]
        col_to_idx = {name: idx for idx, name in enumerate(headers)}

        # 2) Kiểm tra metadata cover tất cả header
        for col in headers:
            if col not in self.column_types:
                text_stream.close()
                mm.close()
                raise ValueError(f"Column '{col}' missing in metadata.")

        # 3) Xây dựng row_filter từ AST (nếu có)
        if ast is not None:
            expr_str = self._ast_to_python_expr(ast, col_to_idx, self.column_types)
            code = f"def row_filter(vals):\n    return {expr_str}"
            namespace: dict[str, Any] = {}
            exec(code, namespace)
            row_filter = namespace["row_filter"]
        else:
            row_filter = lambda vals: True

        # 4) Xác định select_cols và select_idxs, rồi build cast_plan
        if columns == ["*"]:
            select_cols = headers[:]
        else:
            select_cols = columns[:]
            for c in select_cols:
                if c not in col_to_idx:
                    text_stream.close()
                    mm.close()
                    raise ValueError(f"Selected column '{c}' not in CSV header.")
        select_idxs = [col_to_idx[c] for c in select_cols]

        # Build cast_plan: mỗi phần tử là (idx_in_row, cast_fn, column_name)
        cast_plan: list[tuple[int, Callable[[str], Any], str]] = []
        type_map = self.column_types
        type_to_fn = self._type_to_cast_fn
        for col_name, idx in zip(select_cols, select_idxs):
            fn = type_to_fn[type_map[col_name]]
            cast_plan.append((idx, fn, col_name))

        # 5) Duyệt từng dòng qua csv.reader, filter + cast rồi yield JSON
        local_filter = row_filter
        local_cast_plan = cast_plan
        local_select_cols = select_cols

        for vals in reader:
            # Nếu row rỗng hoặc thiếu cột, bỏ qua
            if not vals or len(vals) < len(headers):
                continue

            # Áp dụng filter
            try:
                if not local_filter(vals):
                    continue
            except Exception:
                continue

            # Nếu thỏa điều kiện, cast theo cast_plan
            try:
                typed_vals = [cast_fn(vals[idx]) for idx, cast_fn, _ in local_cast_plan]
                out = dict(zip(local_select_cols, typed_vals))
            except Exception:
                continue

            yield json.dumps(out)

        # Đóng TextIOWrapper và mmap khi kết thúc
        text_stream.close()
        mm.close()

    def _ast_to_python_expr(self, node: Any, col_to_idx: dict[str,int], column_types: dict[str,str]) -> str:
        """
        Đệ quy chuyển ExpressionNode thành một Python boolean expression (chuỗi).
        """
        def recurse(n: ExpressionNode) -> str:
            # Leaf node
            if n.left is None and n.right is None:
                v = n.value
                if isinstance(v, str) and v in col_to_idx:
                    idx = col_to_idx[v]
                    ctype = column_types[v]
                    if ctype == "integer":
                        return f"(int(vals[{idx}]) if vals[{idx}] != '' else 0)"
                    elif ctype == "float":
                        return f"(float(vals[{idx}]) if vals[{idx}] != '' else 0.0)"
                    else:
                        return f"(vals[{idx}].strip())"
                if isinstance(v, (int, float)):
                    return repr(v)
                if isinstance(v, str):
                    return repr(v)
                raise ValueError(f"Unsupported leaf in AST: {v!r}")

            # Nếu chỉ có left (NOT)
            if n.right is None:
                inner = recurse(n.left)
                return f"(not ({inner}))"

            # Nếu có left và right (binary op)
            left_s = recurse(n.left)
            right_s = recurse(n.right)
            op = n.value.upper()
            if op == "AND":
                return f"(({left_s}) and ({right_s}))"
            elif op == "OR":
                return f"(({left_s}) or ({right_s}))"
            elif op in ("=", "=="):
                return f"(({left_s}) == ({right_s}))"
            elif op in ("<>", "!="):
                return f"(({left_s}) != ({right_s}))"
            elif op == ">":
                return f"(({left_s}) > ({right_s}))"
            elif op == "<":
                return f"(({left_s}) < ({right_s}))"
            elif op == ">=":
                return f"(({left_s}) >= ({right_s}))"
            elif op == "<=":
                return f"(({left_s}) <= ({right_s}))"
            elif op == "+":
                return f"(({left_s}) + ({right_s}))"
            elif op == "-":
                return f"(({left_s}) - ({right_s}))"
            elif op == "*":
                return f"(({left_s}) * ({right_s}))"
            elif op == "/":
                return f"(({left_s}) / ({right_s}))"
            elif op == "%":
                return f"(({left_s}) % ({right_s}))"
            else:
                raise ValueError(f"Unsupported operator: {op}")

        return recurse(node)