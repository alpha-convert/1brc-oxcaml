#include <caml/alloc.h>
#include <caml/memory.h>
#include <caml/bigarray.h>
#include <string.h>

CAMLprim int64_t unboxed_ptr_to_offset(const char *start, int64_t v_pos, const char *r) {
  if (!r) return -1;
  int64_t ret = v_pos + r - start;
  return ret;
}

CAMLprim int64_t unboxed_bigstring_find(value v_str, value v_needle, int64_t v_pos, int64_t v_len)
{
  char *start = (char *) Caml_ba_data_val(v_str) + v_pos;
  char *r = (char*) memchr(start, Int_val(v_needle), v_len);
  return unboxed_ptr_to_offset(start, v_pos, r);
}