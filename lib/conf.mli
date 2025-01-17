type t

val create : dir:string -> dbfilename:string -> t
val dir : t -> string
val dbfilename : t -> string
