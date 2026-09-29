import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:dio/dio.dart';
import 'package:web_socket_channel/io.dart';

class WebsocketInfo {
  StreamSubscription steam;
  IOWebSocketChannel ws;
  WebsocketInfo({required this.steam, required this.ws});
}

class Request {
  static String _domain = 'fishpi.cn';
  static String _protocol = 'https';
  static String _parseUrl(String url, Map<String, dynamic>? params) {
    if (params != null) {
      url = '$url?';
      params.forEach((key, value) {
        if (value != null) url += '$key=$value&';
      });
      url = url.substring(0, url.length - 1);
    }
    return url;
  }

  static Future<T> get<T>(String url, {Map<String, dynamic>? params, Map<String, dynamic>? headers}) async {
    return request(_parseUrl(url, params), method: 'GET', headers: headers);
  }

  static Future<T> post<T>(String url, {Map<String, dynamic>? params, dynamic data, Map<String, dynamic>? headers}) async {
    return request(_parseUrl(url, params), method: 'POST', data: data, headers: headers);
  }

  static Future<T> delete<T>(String url, {Map<String, dynamic>? params, dynamic data, Map<String, dynamic>? headers}) async {
    return request(_parseUrl(url, params), method: 'DELETE', data: data, headers: headers);
  }

  static Future<T> put<T>(String url, {Map<String, dynamic>? params, dynamic data, Map<String, dynamic>? headers}) async {
    return request(_parseUrl(url, params), method: 'PUT', data: data, headers: headers);
  }

  static Future<T> request<T>(String url, {method, data, Map<String, dynamic>? headers}) async {
    try {
      var dio = Dio();
      var requestUrl = url.startsWith('http://') || url.startsWith('https://')
          ? url
          : '$_protocol://$_domain/$url';
      var response = await dio.request(requestUrl, data: data, options: Options(method: method, headers: headers));
      if (response.statusCode == 200 || response.statusCode == 201) {
        try {
          if (response.data is Map) {
            return response.data;
          } else {
            try {
              return json.decode(response.data);
            } catch (e) {
              return response.data;
            }
          }
        } catch (e) {
          return Future.error('解析响应数据异常');
        }
      } else if (response.statusCode == 401) {
        return Future.error('401');
      } else {
        return Future.error('HTTP错误');
      }
    } on DioException catch (e) {
      if (e.response?.data != null) {
        var respData = e.response!.data;
        if (respData is Map && respData['msg'] != null) {
          return Future.error(respData['msg']);
        }
      }
      return Future.error(e);
    } catch (e) {
      return Future.error(e);
    }
  }

  static WebsocketInfo connect(
    String url, {
    Map? params,
    required void Function(dynamic msg) onMessage,
    void Function(dynamic error, IOWebSocketChannel ws)? onError,
    void Function(IOWebSocketChannel ws)? onClose,
  }) {
    if (params != null) {
      url = '$url?';
      params.forEach((key, value) {
        url += '$key=$value&';
      });
      url = url.substring(0, url.length - 1);
    }

    url = url.startsWith('ws') ? url : '${_protocol == 'https' ? 'wss' : 'ws'}://$_domain/$url';

    var ws = IOWebSocketChannel.connect(url);
    return WebsocketInfo(
      steam: ws.stream.listen(
        (message) async {
          var msg = message;
          try {
            msg = json.decode(msg);
            // ignore: empty_catches
          } catch (e) {}
          onMessage(msg);
        },
        onDone: onClose == null ? () => print('WebSocket disconnected') : () => onClose(ws),
        onError: onError == null ? (error) => print('WebSocket error: $error') : (error) => onError(error, ws),
      ),
      ws: ws,
    );
  }

  static Future<FormData> formData(String key, {Map<String, dynamic>? src, List<dynamic>? files, String? value}) async {
    src ??= {};
    if (files != null) {
      src[key] = await Future.wait(files.map((f) async {
        if (f is MultipartFile) {
          return f;
        } else if (f is File) {
          return await MultipartFile.fromFile(f.path);
        } else {
          return await MultipartFile.fromFile(f.toString());
        }
      }));
    } else {
      src[key] = value;
    }
    return FormData.fromMap(src);
  }

  static setDomain({required String domain, protocol = 'https'}) {
    _domain = domain;
    _protocol = protocol;
  }

  static get origin => '$_protocol://$_domain';

  static get domain => _domain;
}
