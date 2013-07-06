-module(test,[Name, Fun]).

-export([test1/1,test2/2, test3/2, test4/3, test5/3]).


test1(Message)->
   Fun:format("test1 ~s~n",[Message]),
   
   {ok,Message}.

test2(Message,Arg)->
   Fun:format("test2 ~s ~s~n",[Message,Arg]),
   {ok,Message, Arg}. 

test3(Message,M)->
   M:format("test2 ~s ~n",[Message]),
   {ok,Message, M}. 

test4(Message,M,F)->
   M:F("test2 ~s ~n",[Message]),
   {ok,Message, M}.

test5(Message,M,F)->
   erlang:apply(M,F,[Message]),
   {ok, Message, M, F}. 
