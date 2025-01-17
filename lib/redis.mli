module Conf = Conf

val respond_to : string -> Conf.t -> string
(** [respond_to req] processes the Redis request [req] and returns a serialized
    response. *)
