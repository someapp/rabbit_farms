-module(test).

-export([test1/1,test2/2]).


test1(Message)->
   io:format("test1 ~s",[Message]),
   {ok,Message}.

test2(Message,Arg)->
   io:format("test2 ~s ~s",[Message,Arg]),
   {ok,Message, Arg}. 

