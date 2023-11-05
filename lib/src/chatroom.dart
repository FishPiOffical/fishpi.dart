import 'dart:async';

import 'package:fishpi/fishpi.dart';
import 'package:fishpi/src/request.dart';
import 'package:web_socket_channel/io.dart';

class Chatroom {
  String _apiKey = '';
  String _discusse = '';
  List<dynamic> _onlines = [];

  final ChatSource client = ChatSource();
  WebsocketInfo? _ws;
  List<ChatroomListener> _wsCallbacks = [];

  Redpacket redpacket = Redpacket();

  String get token => _apiKey;

  set token(String token) {
    _apiKey = token;
    redpacket.token = token;
  }

  Chatroom([String? token]) {
    this.token = token ?? this.token;
  }

  /// 当前在线人数列表，需要先调用 addListener 添加聊天室消息监听
  get onlines => _onlines;

  /// 当前聊天室话题，需要先调用 addListener 添加聊天室消息监听
  get discusse => _discusse;

  /// 設置当前聊天室话题
  set discusse(val) {
    send('[setdiscuss]$val[/setdiscuss]');
  }

  Future<ResponseResult> send(String msg, {ChatSource? client}) async {
    try {
      var rsp = await Request.post(
        'chat-room/send',
        data: {
          'content': msg,
          'client': (client ?? this.client).toString(),
          'apiKey': _apiKey,
        },
      );

      return ResponseResult.from(rsp);
    } catch (e) {
      return Future.error(e);
    }
  }

  /// 查询聊天室历史消息
  ///
  /// - `page` 消息页码
  /// - `type` 消息类型，可选值：html、text
  ///
  /// 返回消息列表
  Future<List<ChatRoomMessage>> more(int page,
      {String type = ChatContentType.HTML}) async {
    try {
      var rsp = await Request.get(
        'chat-room/more',
        params: {
          'page': page,
          'type': type,
          'apiKey': token,
        },
      );

      if (rsp['code'] != 0) return Future.error(rsp['msg']);

      return List.from(rsp['data'] ?? [])
          .map((e) => ChatRoomMessage.from(e))
          .toList();
    } catch (e) {
      return Future.error(e);
    }
  }

  /// 获取聊天室消息
  ///
  /// - `oId` 消息 ID
  /// - `mode` 消息类型，可选值：all、before、after
  /// - `size` 消息数量
  /// - `type` 消息内容类型，可选值：html、md
  ///
  /// 返回消息列表
  Future<List<ChatRoomMessage>> get({
    required String oId,
    required ChatMessageType mode,
    int size = 25,
    String type = ChatContentType.HTML,
  }) async {
    try {
      var rsp = await Request.get(
        'chat-room/getMessage',
        params: {
          'oId': oId,
          'mode': mode.toString(),
          'size': size,
          'type': type.toString(),
          'apiKey': _apiKey,
        },
      );

      if (rsp['code'] != 0) return Future.error(rsp['msg']);

      return List.from(rsp['data'] ?? [])
          .map((e) => ChatRoomMessage.from(e))
          .toList();
    } catch (e) {
      return Future.error(e);
    }
  }

  /// 撤回消息，普通成员 24 小时内可撤回一条自己的消息，纪律委员/OP/管理员角色可以撤回任意人消息
  ///
  /// - `oId` 消息 Id
  ///
  /// 返回操作结果
  Future<ResponseResult> revoke(String oId) async {
    try {
      var rsp = await Request.delete(
        'chat-room/revoke/$oId',
        data: {
          'apiKey': _apiKey,
        },
      );

      return ResponseResult.from(rsp);
    } catch (e) {
      return Future.error(e);
    }
  }

  /// 发送一条弹幕
  ///
  /// - `msg` 消息内容，支持 Markdown
  /// - `color` 弹幕颜色
  ///
  /// 返回操作结果
  Future<ResponseResult> barrage(String msg, {String color = '#ffffff'}) async {
    try {
      var rsp = await Request.post(
        'chat-room/send',
        data: {
          'content': '[barrager]{"color":"$color","content":"$msg"}[/barrager]',
          'apiKey': _apiKey,
        },
      );

      return ResponseResult.from(rsp);
    } catch (e) {
      return Future.error(e);
    }
  }

  /// 获取弹幕发送价格
  ///
  /// 返回价格 `cost` 与单位 `unit`
  Future<BarrageCost> barragePay() async {
    try {
      var rsp = await Request.get('cr', params: {'apiKey': _apiKey});

      var match = RegExp(r'>发送弹幕每次将花费\s*<b>([-0-9]+)<\/b>\s*([^<]*?)<\/div>')
          .firstMatch(rsp);

      if (match != null) {
        return BarrageCost(
          cost: int.parse(match.group(1) ?? '20'),
          unit: match.group(2) ?? '积分',
        );
      }

      return BarrageCost(
        cost: 20,
        unit: '积分',
      );
    } catch (e) {
      return Future.error(e);
    }
  }

  /// 获取禁言中成员列表（思过崖）
  ///
  /// 返回禁言中成员列表
  Future<List<MuteItem>> mutes() async {
    try {
      var rsp = await Request.get('chat-room/si-guo-list');

      if (rsp['code'] != null) {
        return Future.error(rsp['msg']);
      }

      return List.from(rsp['data'] ?? []).map((e) => MuteItem.from(e)).toList();
    } catch (e) {
      return Future.error(e);
    }
  }

  /// 获取消息原文（比如 Markdown）
  ///
  /// - `oId` 消息 Id
  Future<String> raw(String oId) async {
    try {
      var rsp = await Request.get('cr/raw/$oId');

      return rsp.replaceAll(RegExp(r'<!--.*?-->'), '');
    } catch (e) {
      return Future.error(e);
    }
  }

  Future reconnect(
      {int timeout = 10, Function(dynamic)? error, Function? close}) async {
    if (_ws != null) {
      _ws?.steam.cancel();
      _ws?.ws.sink.close();
    }
    _ws = Request.connect(
      'chat-room-channel',
      params: {'apiKey': _apiKey},
      onMessage: (msg) {
        dynamic data;
        switch (msg['type']) {
          case ChatRoomMessageType.online:
            {
              _onlines = List.from(msg['users'])
                  .map((e) => OnlineInfo.from(e))
                  .toList();
              _discusse = msg['discussing'];
              data = _onlines;
              break;
            }
          case ChatRoomMessageType.discussChanged:
            {
              data = msg['newDiscuss'];
              break;
            }
          case ChatRoomMessageType.revoke:
            {
              data = msg['oId'];
              break;
            }
          case ChatRoomMessageType.barrager:
            {
              data = BarragerMsg.from(msg);
              break;
            }
          case ChatRoomMessageType.msg:
            {
              data = ChatRoomMsg.from(msg);
              msg['type'] = data.isRedpacket
                  ? ChatRoomMessageType.redPacket
                  : msg['type'];
              break;
            }
          case ChatRoomMessageType.redPacketStatus:
            {
              data = RedPacketStatusMsg.from(msg);
              break;
            }
          case ChatRoomMessageType.custom:
            {
              data = msg['message'];
              break;
            }
          default:
            {
              data = msg;
              break;
            }
        }
        for (ChatroomListener call in _wsCallbacks) {
          call(Message(msg['type'], data));
        }
      },
      onClose: (IOWebSocketChannel ws) => {
        Timer(Duration(milliseconds: timeout), () {
          if (close != null) close();
          ws.sink.close();
          _ws?.steam.cancel();
          _ws = null;
          reconnect(timeout: timeout, error: error);
        }),
      },
      onError: (error, ws) => {if (error != null) error(error)},
    );
  }

  void clearListener() {
    _wsCallbacks = [];
  }

  void removeListener(Function(Message) wsCallback) {
    _wsCallbacks.remove(wsCallback);
  }

  Future addListener(Function(Message) wsCallback,
      {int timeout = 10, Function(dynamic)? error, Function? close}) async {
    if (_ws != null && !_wsCallbacks.contains(wsCallback)) {
      _wsCallbacks.add(wsCallback);
      return;
    }
    _wsCallbacks.add(wsCallback);
    await reconnect(timeout: timeout, error: error, close: close);
  }
}
