module type T = sig
  val compute : measurements:string -> outfile:string -> unit
  val name : string
end