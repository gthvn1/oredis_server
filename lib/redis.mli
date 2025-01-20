module Conf = Conf
module Rdb = Rdb

val respond_to : string -> string
(** [respond_to req] processes the Redis request [req] and returns a serialized
    response. *)
