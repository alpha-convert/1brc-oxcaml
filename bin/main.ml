open! Core
open! Core_bench


module type Impl = One_brc.One_brc_intf.T

let num = "100mm"

let bench i =
  let module I = (val i : Impl) in
  let name = I.name in
  let measurements = "meas-" ^ num ^ ".txt" in
  let outfile = "res-" ^ num ^ "-" ^ name ^ ".txt" in
  Bench.Test.create ~name:(name ^ " " ^ num)
    (fun () -> I.compute ~measurements ~outfile)


let impls : ((module Impl) * string option) list = [
  (* ((module One_brc.Reference), None);
  ((module One_brc.Imperative), Some "reference");
  ((module One_brc.Unboxed), Some "reference");
  ((module One_brc.Mmap), Some "reference");
  ((module One_brc.Fixed_precision), None);
  ((module One_brc.Key_by_hash), Some "fixed_precision");
  ((module One_brc.Branchless), Some "fixed_precision");
  ((module One_brc.Par), Some "fixed_precision");
  ((module One_brc.No_sequence), Some "fixed_precision");
  ((module One_brc.Packed_state), Some "fixed_precision"); *)
  (* ((module One_brc.Swar_temp), Some "fixed_precision");
  ((module One_brc.One_less_memchr), Some "fixed_precision");
  ((module One_brc.Hash_and_search), Some "fixed_precision");
  ((module One_brc.Swar_hash), Some "fixed_precision");
  ((module One_brc.Manual_par), Some "fixed_precision");
  ((module One_brc.Faster_word_hash), Some "fixed_precision");
  ((module One_brc.Lookup_or_null), Some "fixed_precision"); *)
  (* ((module One_brc.Stop_last_word), Some "fixed_precision"); *)
  ((module One_brc.Unboxed2), Some "fixed_precision");
]

let check_all () =
  List.iter impls ~f:(fun (i, ref_name) ->
    let module I = (val i : Impl) in
    match ref_name with
    | None -> ()
    | Some ref_name ->
      let outfile = "res-" ^ num ^ "-" ^ I.name ^ ".txt" in
      let ref_file = "res-" ^ num ^ "-" ^ ref_name ^ ".txt" in
      let contents = In_channel.read_all outfile in
      let expected = In_channel.read_all ref_file in
      if String.(contents <> expected) then
        printf "MISMATCH: %s\n" I.name
      else
        printf "OK: %s\n" I.name
  )

let () = Command_unix.run (
  Bench.make_command (List.map impls ~f:(fun (i, _) -> bench i))
)

let _ = check_all ()