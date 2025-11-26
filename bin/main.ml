open! Core
open! Core_bench


module type Impl = One_brc.One_brc_intf.T

let bench i num =
  let module I = (val i : Impl) in
  let name = I.name in
  let measurements = "meas-" ^ num ^ ".txt" in
  let outfile = "res-" ^ num ^ "-" ^ name ^ ".txt" in
  Bench.Test.create ~name:(name ^ " " ^ num)
    (fun () -> I.compute ~measurements ~outfile)

let () = Command_unix.run (
  Bench.make_command [
    bench (module One_brc.Reference) "1mm";
    bench (module One_brc.Imperative) "1mm";
    bench (module One_brc.Unboxed) "1mm";
    bench (module One_brc.Mmap) "1mm";
    bench (module One_brc.Fixed_precision) "1mm";
    bench (module One_brc.Key_by_hash) "1mm";
    bench (module One_brc.Branchless) "1mm";
  ]
)