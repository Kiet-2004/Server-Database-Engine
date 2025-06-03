import os
import io
import csv
import json
import mmap
from typing import Any, Callable
from server.config.settings import STORAGE_FOLDER
from server.database.entities.ast import ExpressionNode
from server.utils.exceptions import dpapi2_exception
 
# =========================================
# Hàm cast module-level để tránh lỗi pickle hoặc exec lặp
# =========================================
 
def cast_int(raw: str) -> int:
    try:
        return int(raw) if raw != "" else 0
    except ValueError as e:
        raise dpapi2_exception.DataError(f"Cannot cast '{raw}' to integer.") from e
 
def cast_float(raw: str) -> float:
    try:
        return float(raw) if raw != "" else 0.0
    except ValueError as e:
        raise dpapi2_exception.DataError(f"Cannot cast '{raw}' to float.") from e
 
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
        try:
            return self.mm.read(size)
        except (ValueError, BufferError) as e:
            raise dpapi2_exception.InternalError("Error reading from memory-mapped file.") from e
 
    def readline(self, size: int = -1) -> bytes:
        try:
            return self.mm.readline(size)
        except (ValueError, BufferError) as e:
            raise dpapi2_exception.InternalError("Error reading line from memory-mapped file.") from e
 
    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        try:
            return self.mm.seek(offset, whence)
        except (ValueError, OSError) as e:
            raise dpapi2_exception.InterfaceError("Error seeking in memory-mapped file.") from e
 
    def tell(self) -> int:
        try:
            return self.mm.tell()
        except (ValueError, OSError) as e:
            raise dpapi2_exception.InternalError("Error getting position in memory-mapped file.") from e
 
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
        try:
            fd = os.open(self.csv_path, os.O_RDONLY)
        except OSError as e:
            raise dpapi2_exception.OperationalError(f"Cannot open CSV file at '{self.csv_path}'.") from e
        try:
            # length=0 để ánh xạ toàn bộ file
            mm = mmap.mmap(fd, length=0, access=mmap.ACCESS_READ)
        except (ValueError, OSError) as e:
            os.close(fd)
            raise dpapi2_exception.InternalError(f"Cannot memory-map file '{self.csv_path}'.") from e
        os.close(fd)
        return mm
 
    def select(self, columns: list[str], ast: Any = None):
        """
        - columns: list tên cột user muốn SELECT (hoặc ["*"] để lấy tất cả).
        - ast: ExpressionNode (cây điều kiện WHERE). Nếu None, chọn tất cả hàng.
 
        Trả về một generator, mỗi yield là JSON string (đã lọc + cast).
        """
        try:
            mm = self._open_mmap()
            mmap_reader = MMapReader(mm)
            text_stream = io.TextIOWrapper(mmap_reader, encoding="utf-8", newline="")
            reader = csv.reader(text_stream)
        except dpapi2_exception.Error:
            # Đã convert thành DBAPI2 exception trong _open_mmap hoặc MMapReader
            raise
        except Exception as e:
            raise dpapi2_exception.OperationalError("Error initializing CSV reader.") from e
 
        try:
            # Đọc header
            try:
                headers = next(reader)
            except StopIteration:
                raise dpapi2_exception.OperationalError("CSV file is empty.")
            headers = [h.strip() for h in headers]
            col_to_idx = {name: idx for idx, name in enumerate(headers)}
 
            # Kiểm tra metadata cover tất cả header
            for col in headers:
                if col not in self.column_types:
                    raise dpapi2_exception.ProgrammingError(f"Column '{col}' missing in metadata.")
 
            # Xây dựng row_filter từ AST (nếu có)
            if ast is not None:
                try:
                    expr_str = self._ast_to_python_expr(ast, col_to_idx, self.column_types)
                    code = f"def row_filter(vals):\n    return {expr_str}"
                    namespace: dict[str, Any] = {}
                    exec(code, namespace)
                    row_filter = namespace["row_filter"]
                except dpapi2_exception.ProgrammingError:
                    raise
                except Exception as e:
                    raise dpapi2_exception.ProgrammingError("Error compiling WHERE expression.") from e
            else:
                row_filter = lambda vals: True
 
            # Xác định select_cols và select_idxs, rồi build cast_plan
            if columns == ["*"]:
                select_cols = headers[:]
            else:
                select_cols = columns[:]
                for c in select_cols:
                    if c not in col_to_idx:
                        raise dpapi2_exception.ProgrammingError(f"Selected column '{c}' not in CSV header.")
            select_idxs = [col_to_idx[c] for c in select_cols]
 
            # Build cast_plan: mỗi phần tử là (idx_in_row, cast_fn, column_name)
            cast_plan: list[tuple[int, Callable[[str], Any], str]] = []
            type_map = self.column_types
            type_to_fn = self._type_to_cast_fn
            for col_name, idx in zip(select_cols, select_idxs):
                col_type = type_map.get(col_name)
                if col_type not in type_to_fn:
                    raise dpapi2_exception.NotSupportedError(f"Unsupported column type '{col_type}' for column '{col_name}'.")
                fn = type_to_fn[col_type]
                cast_plan.append((idx, fn, col_name))
 
            # 5) Duyệt từng dòng qua csv.reader, filter + cast rồi yield JSON
            for vals in reader:
                # Nếu row rỗng hoặc thiếu cột
                if not vals or len(vals) < len(headers):
                    continue
 
                # Áp dụng filter
                try:
                    passed = row_filter(vals)
                except Exception as e:
                    raise dpapi2_exception.ProgrammingError("Error evaluating WHERE filter.") from e
                if not passed:
                    continue
 
                # Cast theo cast_plan
                try:
                    typed_vals = [cast_fn(vals[idx]) for idx, cast_fn, _ in cast_plan]
                except dpapi2_exception.DataError:
                    # Casting từng cột đã raise DataError nếu lỗi, propagate
                    raise
                except Exception as e:
                    raise dpapi2_exception.DataError("Error casting row values.") from e
 
                # Build output map và yield
                out = dict(zip(select_cols, typed_vals))
                yield json.dumps(out)
 
        finally:
            # Đóng TextIOWrapper và mmap khi kết thúc hoặc lỗi
            try:
                text_stream.close()
            except Exception:
                pass
            try:
                mm.close()
            except Exception:
                pass
 
    def _ast_to_python_expr(self, node: Any, col_to_idx: dict[str,int], column_types: dict[str,str]) -> str:
        """
        Đệ quy chuyển ExpressionNode thành một Python boolean expression (chuỗi).
        """
        def recurse(n: ExpressionNode) -> str:
            # Leaf node
            if n.left is None and n.right is None:
                v = n.value
                # Nếu là identifier (cột)
                if isinstance(v, str) and v in col_to_idx:
                    idx = col_to_idx[v]
                    ctype = column_types[v]
                    if ctype == "integer":
                        return f"(int(vals[{idx}]) if vals[{idx}] != '' else 0)"
                    elif ctype == "float":
                        return f"(float(vals[{idx}]) if vals[{idx}] != '' else 0.0)"
                    elif ctype == "string":
                        return f"(vals[{idx}].strip())"
                    else:
                        raise dpapi2_exception.ProgrammingError(f"Unsupported column type '{ctype}' for '{v}'.")
                # Nếu là literal số
                if isinstance(v, (int, float)):
                    return repr(v)
                # Nếu là literal chuỗi (đã được bao ngoặc) -> node.value truyền vào phải là string đã loại nháy
                if isinstance(v, str) and len(v) >= 2 and ((v[0] == v[-1] == '"') or (v[0] == v[-1] == "'")):
                    return repr(v[1:-1])
                # Không phải identifier hợp lệ hay literal hợp lệ
                raise dpapi2_exception.ProgrammingError(f"Invalid literal or column '{v}'.")
 
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
                raise dpapi2_exception.NotSupportedError(f"Unsupported operator: {op}")
 
        return recurse(node)