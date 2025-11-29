open! Base
open! Core
open! Core_unix

let name = "reference"

module Record : sig
  type t = {
    count : int;
    min : float;
    max : float;
    tot : float 
  } [@@deriving sexp]

  val merge : t -> t -> t
  val unit : float -> t
end
= struct
  type t = {
    count : int;
    min : float;
    max : float;
    tot : float 
  } [@@deriving  sexp]

  let merge r r' = {
    count = r.count + r'.count;
    min = Float.min r.min r'.min;
    max = Float.max r.max r'.max;
    tot= Float.(r.tot + r'.tot);
  }

  let unit temp = {
    count = 1;
    min = temp;
    max = temp;
    tot = temp;
  }
end

let process_file lines =
  let update_or_add temp = function
    | None -> Record.unit temp
    | Some record -> Record.merge record (Record.unit temp)
  in
  List.fold lines ~init:(String.Map.empty) ~f:(fun records line ->
    let (town,temp_s) = String.lsplit2_exn line ~on:';' in
    let temp = Float.of_string temp_s in
    Map.update records town ~f:(update_or_add temp)
  )

let compute ~measurements ~outfile =
    let fcontents = In_channel.read_all measurements in
    let lines = String.split_lines fcontents in
    let res = process_file lines in
    let ofd = Out_channel.create outfile in
    Map.iteri res ~f:(fun ~key ~data ->
      let mean = Float.(data.tot / (of_int data.count)) in
      Out_channel.output_string ofd (Printf.sprintf "%s=%.1f/%.1f/%.1f\n" key data.min mean data.max)
    );
