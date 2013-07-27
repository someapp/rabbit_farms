-module(password).

-export([encode_password/1, 
	 encode_password/2,
	 decode_password/1,
	 decode_password/2, 
	 is_secure/1,
	 is_secure/2]).

-spec encode_password(term())-> binary().
encode_password(Value) ->
	A = base64:encode(Value),
	A1 = base64:encode(A),
	base64:encode(A1).

-spec encode_password(term(), atom()) -> bitstring().	
encode_password(Value, Mod) when is_atom(Mod)->
	A = Mod:encode(Value),
	A1 = Mod:encode(A),
	Mod:encode(A1).

-spec decode_password(binary())-> term().
decode_password(Value)->
	B1 = base64:decode(Value),
	B2 = base64:decode(B1),
	base64:decode(B2).

-spec decode_password(binary(), atom()) -> bitstring().	
decode_password(Value, Mod) when is_atom(Mod)->
	B1 = Mod:decode(Value),
	B2 = Mod:decode(B1),
	Mod:decode(B2).

-spec is_secure(term())-> true | false.
is_secure(Value) ->
	R = encode_password(Value),
	Value =:= decode_password(R).

-spec is_secure(term(), atom()) -> bitstring().	
is_secure(Value, Mod) when is_atom(Mod)->
	R = encode_password(Value, Mod),
	Value =:= decode_password(R, Mod).
