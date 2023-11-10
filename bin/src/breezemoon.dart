import 'dart:io';
import 'dart:math';

import 'user.dart';
import 'base.dart';

class BreezemoonCmd implements CommandInstance {
  int _page = 1;
  int _size = 20;
  @override
  ArgParser command(ArgParser parser) {
    return parser..addOption('breezemoon', help: '发送清风明月');
  }

  @override
  Future<void> exec(ArgResults args, PrintFn print) async {
    if (args['breezemoon'] != null) {
      if (!Instance.get.isLogin && !await UserCmd().login()) {
        exit(0);
      }
      Instance.get.breezemoon.send(args['breezemoon']).then(print);
      exit(0);
    }
  }

  @override
  Future<bool> call(String command) async {
    if (command.trim().isEmpty) return false;
    var argv = command.trim().split(' ');
    switch (argv[0]) {
      case ':to':
        {
          if (argv.length < 2) {
            try {
              stdout.write('要跳转到哪一页：');
              _page = int.parse(stdin.readLineSync() ?? '1');
            } catch (e) {
              _page = 1;
            }
          }
          await page(':page breezemoon');
          break;
        }
      case ':size':
        {
          if (argv.length < 2) {
            try {
              stdout.write('要显示多少笔：');
              _size = int.parse(stdin.readLineSync() ?? '20');
            } catch (e) {
              _size = 20;
            }
          }
          await page(':page breezemoon');
          break;
        }
      case ':next':
        {
          _page++;
          await page(':page breezemoon');
          break;
        }
      case ':prev':
        {
          _page = max(1, _page - 1);
          await page(':page breezemoon');
          break;
        }
      default:
        {
          if (!Instance.get.isLogin) {
            print('请先登录。');
            break;
          }
          if (!Platform.isWindows) {
            stdout.write('是否要发送清风明月$command？[y/N]');
            var answer = stdin.readLineSync();
            if (answer?.toLowerCase() == 'y') {
              await Instance.get.breezemoon.send(command);
              await page(':page breezemoon 1');
            }
          } else {
            print('命令发送消息不支援 Windows 端。请使用 --breezemoon 命令行参数发送。');
          }
        }
    }

    return false;
  }

  @override
  Future<bool> page(String command) async {
    int page = _page, size = _size;
    final commands = command.trim().split(' ');
    if (commands.length > 2 && commands[2].isNotEmpty) {
      page = int.parse(commands[2]);
    }
    if (commands.length > 3 && commands[3].isNotEmpty) {
      size = int.parse(commands[3]);
    }

    Instance.get.breezemoon.list(page: page, size: size).then((list) {
      for (var item in list.reversed) {
        print(
            '${Command.bold}${item.breezemoonAuthorName}${Command.restore} ${Command.from('#AAAAAA').color}[${item.timeAgo}]${Command.restore} 📍${item.breezemoonCity}');
        print(
            '${Command.italic}${htmlToText(item.breezemoonContent, userName: Instance.get.user.current.userName)}${Command.restore}');
        print('');
      }
    });
    return false;
  }
}
