-module(password).

-export([encode_password/1,
		 decode_password/1, 
		 is_secure/1]).

-spec encode_password(term())-> binary().
encode_password(Value) ->
	A = base64:encode(Value),
	A1 = base64:encode(A),
	base64:encode(A1).

-spec decode_password(binary())-> term().
decode_password(Value)->
	B1 = base64:decode(Value),
	B2 = base64:decode(B1),
	base64:decode(B2).

-spec is_secure(term())-> true | false.
is_secure(Value) ->
	R = encode_password(Value),
	Value =:= decode_password(R).