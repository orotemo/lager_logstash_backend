-module(lager_logstash_backend).

%% Started from the lager logstash backend
-author('marc.e.campbell@gmail.com').
-author('mhald@mac.com').

-behaviour(gen_event).

-export([init/1,
  handle_call/2,
  handle_event/2,
  handle_info/2,
  terminate/2,
  code_change/3,
  logtime/0,
  get_app_version/0
]).

-record(state, {socket :: pid(),
  lager_level_type :: 'mask' | 'number' | 'unknown',
  level :: atom(),
  logstash_host :: string(),
  logstash_port :: number(),
  logstash_address :: inet:ip_address(),
  node_role :: string(),
  node_version :: string(),
  metadata :: list(),
  logstash_token :: string()
}).

init(Params) ->
  %% we need the lager version, but we aren't loaded, so... let's try real hard
  %% this is obviously too fragile
  {ok, Properties}     = application:get_all_key(),
  {vsn, Lager_Version} = proplists:lookup(vsn, Properties),
  Lager_Level_Type =
    case string:to_float(Lager_Version) of
      {V1, _} when V1 < 2.0 ->
        'number';
      {V2, _} when V2 =:= 2.0 ->
        'mask';
      {_, _} ->
        'unknown'
    end,

  Level = lager_util:level_to_num(proplists:get_value(level, Params, debug)),
  Host = proplists:get_value(logstash_host, Params, "localhost"),
  Port = proplists:get_value(logstash_port, Params, 9125),
  Token = proplists:get_value(logstash_token, Params),
  Node_Role = proplists:get_value(node_role, Params, "no_role"),
  Node_Version = proplists:get_value(node_version, Params, "no_version"),

  Metadata = proplists:get_value(metadata, Params, []) ++
    [
      {pid, [{encoding, process}]},
      {line, [{encoding, integer}]},
      {file, [{encoding, string}]},
      {module, [{encoding, atom}]}
    ],

  {Socket, Address} =
    case inet:getaddr(Host, inet) of
      {ok, Addr} ->
        {ok, Sock} = gen_udp:open(0, [list]),
        {Sock, Addr};
      {error, _Err} ->
        {undefined, undefined}
    end,

  {ok, #state{socket = Socket,
    lager_level_type = Lager_Level_Type,
    level = Level,
    logstash_host = Host,
    logstash_port = Port,
    logstash_address = Address,
    node_role = list_to_binary(Node_Role),
    node_version = list_to_binary(Node_Version),
    metadata = Metadata,
    logstash_token = list_to_binary(Token) }}.

handle_call({set_loglevel, Level}, State) ->
  {ok, ok, State#state{level=lager_util:level_to_num(Level)}};

handle_call(get_loglevel, State) ->
  {ok, State#state.level, State};

handle_call(_Request, State) ->
  {ok, ok, State}.

handle_event({log, _}, #state{socket=S}=State) when S =:= undefined ->
  {ok, State};
handle_event({log, {lager_msg, Q, Metadata, Severity, {Date, Time}, _, Message}}, State) ->
  handle_event({log, {lager_msg, Q, Metadata, Severity, {Date, Time}, Message}}, State);

handle_event({log, {lager_msg, _, Metadata, Severity, {Date, Time}, Message}},
  #state{level=L}=State) ->
  case lager_util:level_to_num(Severity) =< L of
    true ->
      try
        send_message(State, Severity, {Date, Time}, Message, Metadata)
      catch
        Error:Reason  ->
          ErrorFlat = lists:flatten(io_lib:format(
            "Got error durring json log parsing of object ~p, Error=~p, Reason=~p",
            [Metadata, Error, Reason])),
          send_message(State, error, {Date, Time}, ErrorFlat, [])
      end;
    _ -> ok
  end,
  {ok, State};

handle_event(_Event, State) ->
  {ok, State}.

handle_info(_Info, State) ->
  {ok, State}.

terminate(_Reason, #state{socket=S}=_State) ->
  gen_udp:close(S),
  ok;
terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  %% TODO version number should be read here, or else we don't support upgrades
  Vsn = get_app_version(),
  {ok, State#state{node_version=Vsn}}.

send_message(State, Severity, {Date, Time}, Message, Metadata) ->
  Encoded_Message = encode_json_event(node(),
    State#state.node_role,
    State#state.node_version,
    State#state.logstash_token,
    Severity,
    Date,
    Time,
    Message,
    to_proper_ejson(Metadata)),
  gen_udp:send(State#state.socket,
    State#state.logstash_address,
    State#state.logstash_port,
    Encoded_Message).

encode_json_event(Node, Node_Role, Node_Version, Token, Severity, Date, Time,
  Message, {Metadata}) ->
  DateTime = io_lib:format("~sT~s", [Date,Time]),
  Fields = {[
    {<<"version">>, Node_Version},
    {<<"node">>, Node}
  ] ++ Metadata},

  jiffy:encode({[
    {<<"log_level">>, Severity},
    {<<"fields">>, Fields},
    {<<"@timestamp">>, list_to_binary(DateTime)}, %% use the logstash timestamp
    {<<"message">>, safe_list_to_binary(Message)},
    {<<"type">>, Node_Role},
    {<<"token">>, Token}
  ]}).

safe_list_to_binary(L) when is_list(L) ->
  unicode:characters_to_binary(L);
safe_list_to_binary(L) when is_binary(L) ->
  unicode:characters_to_binary(L).

get_app_version() ->
  [App,_Host] = string:tokens(atom_to_list(node()), "@"),
  Apps = application:which_applications(),
  case proplists:lookup(list_to_atom(App), Apps) of
    none ->
      "no_version";
    {_, _, V} ->
      V
  end.

logtime() ->
  {{Year, Month, Day}, {Hour, Minute, Second}} = erlang:universaltime(),
  lists:flatten(io_lib:format("~4.10.0B-~2.10.0B-~2.10.0BT~2.10.0B:~2.10.0B:~2.10.0B.~.10.0BZ",
    [Year, Month, Day, Hour, Minute, Second, 0])).

to_proper_ejson(DS) when is_list(DS) -> {[to_proper_ejson(I) || I <- DS]};
to_proper_ejson({K, V}) -> {to_proper_key(K), to_proper_ejson(V)};
to_proper_ejson(Other) -> Other.

to_proper_key(Key) when is_float(Key) -> float_to_binary(Key);
to_proper_key(Key) when is_integer(Key) -> integer_to_binary(Key);
to_proper_key(Key) -> Key.
