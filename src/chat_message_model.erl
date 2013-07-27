-module(chat_message_model).
-include_lib("chat_message.hrl").
-behaviour(json_rec_model).

-compile({parse_transform, exprecs}).
-export([new/1, 
	 	 rec/1,
	  	 ensure_binary/1]).
-spec new( bitstring() )-> #chat_message{} | undefined.
new(<<"chat_message">>)->
   '#new-chat_message'();
new(_)-> undefined.

rec(#chat_message{} =Value) ->  Value;
rec(_)-> undefined.

-spec ensure_binary(atom() | any()) -> binary().
ensure_binary(#chat_message{} = Value) ->
    Json = json_rec:to_json(Value, chat_message_model),
    lists:flatten(mochijson2:encode(Json));
ensure_binary(Val) -> app_util:ensure_binary(Val).
