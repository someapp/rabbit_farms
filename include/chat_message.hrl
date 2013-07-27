-ifndef(CHAT_MESSAGE_HRL).
-define(CHAT_MESSAGE_HRL, true).
-record(chat_message, {
		from ::bitstring(),
		to  ::bitstring(),
		brandId  ::bitstring(),
		type ::bitstring(), 
		format ::bitstring(),
		subject ::bitstring(), 
		body ::bitstring(), 
		thread ::bitstring(),
		time_stamp ::bitstring()}).


-export_records([chat_message]).


-endif.
