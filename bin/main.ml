open! Core
open! Core_bench


module type Impl = One_brc.One_brc_intf.T

let num = "1mm"

let bench i =
  let module I = (val i : Impl) in
  let name = I.name in
  let measurements = "meas-" ^ num ^ ".txt" in
  let outfile = "res-" ^ num ^ "-" ^ name ^ ".txt" in
  Bench.Test.create ~name:(name ^ " " ^ num)
    (fun () -> I.compute ~measurements ~outfile)


let impls : (module Impl) list = [
  (module One_brc.Reference);
  (module One_brc.Imperative);
  (module One_brc.Unboxed);
  (module One_brc.Mmap);
  (module One_brc.Fixed_precision);
  (module One_brc.Key_by_hash);
  (module One_brc.Branchless);
  (module One_brc.Par);
  (module One_brc.No_sequence);
  (module One_brc.Packed_state);
  (module One_brc.Swar_temp);
  (module One_brc.One_less_memchr);
  (module One_brc.Hash_and_search);
  (module One_brc.Swar_hash);
  (module One_brc.Manual_par);
]

let float_impls = ["reference"; "imperative"; "unboxed"; "mmap"]

let check_all () =
  let reference_file = "res-" ^ num ^ "-reference.txt" in
  let fixed_file = "res-" ^ num ^ "-fixed_precision.txt" in
  let reference = In_channel.read_all reference_file in
  let fixed = In_channel.read_all fixed_file in
  List.iter impls ~f:(fun i ->
    let module I = (val i : Impl) in
    if String.(I.name = "reference" || I.name = "fixed_precision") then ()
    else begin
      let outfile = "res-" ^ num ^ "-" ^ I.name ^ ".txt" in
      let contents = In_channel.read_all outfile in
      let expected = if List.mem float_impls I.name ~equal:String.equal then reference else fixed in
      if String.(contents <> expected) then
        printf "MISMATCH: %s\n" I.name
      else
        printf "OK: %s\n" I.name
    end
  )

let () = Command_unix.run (
  Bench.make_command (List.map impls ~f:(fun i -> bench i))
)

let _ = check_all ()