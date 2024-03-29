%%%-------------------------------------------------------------------
%%% File    : hasbeen.erl
%%% Author  : Tee Teoh <tteoh@teemac.ott.cti.com>
%%% Description :
%%%
%%% Created : 6 Mar 2013 by Gina Hagg>
%%%-------------------------------------------------------------------
-module(hasbeen).
%-export([parse_ff_dump/0, parse_msec_date/1, day_visits/0]).
-compile(export_all).
%% ====================================================================
%% API
%% ====================================================================
%% --------------------------------------------------------------------
%% Function: 
%% Description:
%% --------------------------------------------------------------------
-define (MSecZ , (1000000 * 1000000)).
-define (MIL , 1000000).
%GSecs = calendar:datetime_to_gregorian_seconds({{1970,1,1},{0,0,0}}).
-define (GSECS, 62167219200).

parse_ff_dump()->
    FileName = "/Users/ginahagg/mywork/firefoxhistory1.txt",
    Res = eredis:start_link(),
    {ok, C} = Res,
	%%{ok, [Form]} = epp:parse_file(FileName,[],[]),
    {ok, Data} = file:read_file(FileName),
	L = binary:split(Data,<<"\n">>,[global]),
	lists:map(fun(X)-> 
	    K = binary:split(X,<<"|">>,[global]),
	    %io:format("Heres ~p\n",[K]),
	    case K of
			[<<>>] -> io:format("empty");
		    [P] -> P;
		    [_, P, Q] ->	
		    	%io:format("~p\n",[Q])
		    	{ok,<<"OK">>} = eredis:q(C,["SET",Q,P])
		end     		    		    
		end, L).


%zcount "2013-2-20" -inf +inf
		
parse_to_list()->
    FileName = "/Users/ginahagg/mywork/hasbeenhere/firefoxhistory1.txt",
	%%{ok, [Form]} = epp:parse_file(FileName,[],[]),
    {ok, Data} = file:read_file(FileName),
	L = binary:split(Data,<<"\n">>,[global]),
	Res = eredis:start_link(),
    {ok, C} = Res,
	lists:map(fun(X)-> 
	    K = binary:split(X,<<"|">>,[global]),
	    %io:format("Heres ~p\n",[K]),
	    case K of
			[<<>>] -> io:format("empty");
		    [P] -> P;
		    [_, P, Q] ->	
		    	%io:format("~p\n",[Q])
		    	QQ = list_to_integer(binary_to_list(Q)),
		    	{{Yr,Mn,Dy},_} = parse_msec_date(QQ),
		    	ZsetName = lists:concat([Yr,"-",Mn,"-",Dy]),
		    	io:format("adding ~p to ~p\n",[Q,ZsetName]),
		    	eredis:q(C,["ZADD", ZsetName,Q, P])		    	
		end     		    		    
		end, L).
		
		
		
                      
%{ok,<<"OK">>}


day_visits(Srcword, Dtt)->
    
    Dt = {Dtt,{0,0,0}},   
    Secss = calendar:datetime_to_gregorian_seconds(Dt)-?GSECS,
    ToDt = case Dtt of 
        {A,1,1} ->
        	calendar:datetime_to_gregorian_seconds({{A+1,1,1},{0,0,0}})-?GSECS;
    	{A,B,1} ->
        	calendar:datetime_to_gregorian_seconds({{A,B+1,1},{0,0,0}})-?GSECS;
    	{A,B,C} ->
        	calendar:datetime_to_gregorian_seconds({{A,B,C+11},{0,0,0}})-?GSECS
    end,
    {ok,Con} = eredis:start_link(),
    %1345506891212191	
    %1340591395854251	
	{AA,BB,_} = Src = {1340,591395,212191},
	D = lists:concat([AA,(BB div 1000),"*"]),
	%io:format("Heres ~p",[D]),
	{ok,R} = eredis:q(Con,["KEYS", D ]),
	lists:map(fun(X)-> 
		Keyy = binary_to_list(X),		
		{ok,R1} = eredis:q(Con,["GET", Keyy]),
		R2 = binary_to_list(R1),
		io:format("we found ~p\n",[R2]),
		I = string:str(R2,Srcword),
		case I of 
			0 ->
				io:format("No visits",[]);
			_ -> R2
		end
	end, R).
	
find_srch_key(Dtt) ->
	case Dtt of 
        {A,0,0} ->
        	lists:concat([A,"-","*"]);
    	{A,B,0} ->    	   
        	lists:concat([A,"-",B,"-","*"]);
    	{A,B,C} ->
        	lists:concat([A,"-",B,"-",C])
    end.
    
getvisits(Srcword, Dtt)->   
    Keyz = find_srch_key(Dtt),
    %io:format("Keyz:~p",[Keyz]),
	{ok,Con} = eredis:start_link(),
	{ok,R} = eredis:q(Con,["KEYS", Keyz]),
	lists:map(fun(X)-> 
		Keyy = binary_to_list(X),
		%{ok, Ct} =  eredis:q(Con,["ZCOUNT", Keyy, "-inf" , "+inf"]), 	
		{ok, R1} =  eredis:q(Con,["ZRANGE", Keyy, "0" , "-1"]),          %,"WITHSCORES"]),
		%io:format("~p:::~p\n",[Keyy,R1]),
		FL = lists:map(fun(X) ->			
			has_visited(X,Srcword)
		end,R1)
		%,lists:flatten(FL)
	end,R).
	




dayvisits(Srcword, Dtt)->
    
    Dt = {Dtt,{0,0,0}},   
    Secs = calendar:datetime_to_gregorian_seconds(Dt)-?GSECS,
    ToD = case Dtt of 
        {A,1,1} ->
        	io:format("case 1",[]),
        	calendar:datetime_to_gregorian_seconds({{A+1,1,1},{0,0,0}})-?GSECS;
    	{A,B,1} ->
    	    io:format("case 2",[]),
        	calendar:datetime_to_gregorian_seconds({{A,B+1,1},{0,0,0}})-?GSECS;
    	{A,B,C} ->
    	    io:format("case 3",[]),
        	calendar:datetime_to_gregorian_seconds({{A,B,C+11},{0,0,0}})-?GSECS
    end,
  
	%{AA,BB,_} = Src = {1340,591395,212191},
	ToDt = lists:concat([ToD,"000000"]),
	Secss = lists:concat([Secs,"000000"]),
	%io:format("Heres ~p ||| ~p ||| ~p",[Dtt, Secss, ToDt]),
	{ok,Con} = eredis:start_link(),
	{ok, R} =  eredis:q(Con,["ZRANGEBYSCORE", "ILIST", Secss , ToDt]),  %,"WITHSCORES"]),
	%io:format("zlist: ~p\n",[R]),
	lists:map(fun(X)-> 
		Keyy = binary_to_list(X),		
		%{ok,R1} = eredis:q(Con,["GET", Keyy]),
		%io:format("R1: ~p\n",[R1]),
		R2 = binary_to_list(X),
		%io:format("we found ~p\n",[R2]),
		I = string:str(R2,Srcword),
		case I of 
			0 ->
				[];
			_ -> R2
		end
	end, R).

has_visited(Keyy,Srcword) ->
	    %io:format("Keyy::~p\n",[Keyy]),
	    Keyyy = binary_to_list(Keyy),		
		
		I = string:str(Keyyy,Srcword),
		if I > 0 -> Keyyy;
		true -> ""
		end.
		
		
parse_msec_date(Dte)->
    io:format("msecs: ~p",[Dte]),
	MSz = Dte div ?MSecZ,
	Sz = Dte rem ?MSecZ,
	Sz1 = Sz div ?MIL,
	MinSz = Sz rem ?MIL,
	{Dt, Tm} = calendar:now_to_universal_time({MSz,Sz1,MinSz}),
	{Dt, Tm}.
	
	
	
	
%% Useful info

%LLL = lists:flatten(LL),
%io:format("list:~p",[LLL]).
%{ok,<<"OK">>} = eredis:q(C,["MSET"| LLL]).

%redis.zrangebyscore('GOOG', (time.now.utc - 5).to_i, time.now.utc.to_i)
%redis.zrangebyscore('ILIST', "1340482096526383", "1361334628281280")

%eredis:q(C,["KEYS","134*"]).
%{ok,[<<"1340482096526383">>,<<"1340176541701821">>,
%     <<"1345506891212191">>]}



%105> Dt = {{2013,3,10},{0,0,0}}.
%{{2013,3,10},{0,0,0}}
%106> GSecs = calendar:datetime_to_gregorian_seconds({{1970,1,1},{0,0,0}}).
%62167219200
%107> Secs = calendar:datetime_to_gregorian_seconds(Dt)-GSecs.             
%1362873600
%108> {Secs div 1000000, Secs rem 1000000,0}.
%{1362,873600,0}


%KVP = ["key1","value1","key2","value2"].
%["key1","value1","key2","value2"]
%122> eredis:q(Cdd,["MSET"| KVP]). 	

	    
%zrangebyscore ILIST  "1340482096526383"  "1361334628281280" withscores
%zcount "2013-2-20" -inf +inf
	