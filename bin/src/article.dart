import 'dart:io';
import 'dart:math';

import '../main.dart';
import 'base.dart';

enum ArticlePage { list, detail }

class ArticleCmd implements CommandInstance {
  int _page = 1;
  int _commentPage = 1;
  String _tag = '';
  String _type = ArticleListType.Recent;
  ArticleList _current = ArticleList();
  ArticleDetail _currentDetail = ArticleDetail();
  ArticlePage _currentPage = ArticlePage.list;

  @override
  ArgParser command(ArgParser parser) {
    return parser;
  }

  @override
  Future<void> exec(ArgResults args, PrintFn print) async {}

  @override
  Future<bool> call(String command) async {
    if (command.trim().isEmpty) return false;
    var argv = command.trim().split(' ');
    var replyId = '';
    switch (argv[0]) {
      case ':to':
        {
          try {
            if (argv.length < 2) {
              stdout.write('要跳转到哪一页：');
              if (_currentPage == ArticlePage.list) {
                _page = int.parse(stdin.readLineSync() ?? '1');
              } else {
                _commentPage = int.parse(stdin.readLineSync() ?? '1');
              }
            } else {
              if (_currentPage == ArticlePage.list) {
                _page = int.parse(argv[1]);
              } else {
                _commentPage = int.parse(argv[1]);
              }
            }
          } catch (e) {
            _currentPage == ArticlePage.list ? _page = 1 : _commentPage = 1;
          }
          await page(
              ':page article ${_currentPage == ArticlePage.detail ? _currentDetail.oId : ''}');
          break;
        }
      case ':tag':
        {
          if (argv.length < 2) {
            stdout.write('要查看哪个 Tag：');
            _tag = stdin.readLineSync() ?? '';
          } else {
            _tag = argv[1];
          }
          await page(':page article');
          break;
        }
      case ':view':
        {
          String oId = '';
          try {
            if (argv.length < 2) {
              stdout.write('要查看哪一篇：');
              var index = int.parse(stdin.readLineSync() ?? '1');
              if (index > 0 && index <= _current.articles.length) {
                oId = _current.articles[index - 1].oId;
              } else {
                throw Exception('找不到对应编号或索引的文章');
              }
            } else if (argv[1].length == 13) {
              oId = argv[1];
            } else {
              var index = int.parse(argv[1]);
              oId = _current.articles[index - 1].oId;
            }
            await page(':page article $oId');
          } catch (e) {
            print('找不到对应编号或索引的文章');
          }
          break;
        }
      case ':all':
        {
          _tag = '';
          _page = 1;
          await page(':page article');
          break;
        }
      case ':next':
        {
          if (_currentPage == ArticlePage.list) {
            _page++;
            await page(':page article');
          } else {
            _commentPage++;
            await page(':page article ${_currentDetail.oId}');
          }
          break;
        }
      case ':prev':
        {
          if (_currentPage == ArticlePage.list) {
            _page = max(1, _page - 1);
            await page(':page article');
          } else {
            _commentPage = max(1, _commentPage - 1);
            await page(':page article ${_currentDetail.oId}');
          }
          break;
        }
      case ':type':
        {
          if (argv.length < 2 || ArticleListType.values.contains(argv[1])) {
            stdout.write('要按哪个维度查看文章(${ArticleListType.values.join(', ')})：');
            _type = stdin.readLineSync() ?? '';
          } else {
            _type = argv[1];
          }
          await page(':page article');
          break;
        }
      case ':reply':
        {
          try {
            if (_currentPage != ArticlePage.detail) break;

            if (Platform.isWindows) {
              print('命令发送评论不支援 Windows 端。');
              break;
            }

            if (!Instance.get.isLogin) {
              print('请先登录。');
              break;
            }

            if (argv.length < 2) {
              stdout.write('要回复哪一条评论：');
              var index = int.parse(stdin.readLineSync() ?? '1');
              if (index > 0 && index <= _current.articles.length) {
                replyId = _currentDetail.articleComments[index - 1].oId;
              } else {
                throw Exception('找不到对应编号的评论');
              }
            } else {
              var index = int.parse(argv[1]);
              replyId = _currentDetail.articleComments[index - 1].oId;
            }

            var content = argv.length > 2
                ? argv.skip(2).join(' ')
                : stdin.readLineSync() ?? '';
            await comment(CommentPost(
              articleId: _currentDetail.oId,
              content: content,
              replyId: replyId,
            ));
          } catch (e) {
            print('找不到对应编号的评论');
            break;
          }
          break;
        }
      case ':reward':
        {
          if (_currentPage != ArticlePage.detail) break;
          if (_currentDetail.articleRewardPoint == 0) {
            print('该文章不支持打赏');
            break;
          }
          if (_currentDetail.articleRewardPoint == 0) {
            print('该文章不支持打赏');
            break;
          }
          if (!Instance.get.isLogin) {
            print('请先登录。');
            break;
          }
          if (_currentDetail.rewarded) {
            print('你已经打赏过了。');
            break;
          }
          stdout
              .write('确认要打赏${_currentDetail.articleRewardPoint}积分给作者吗？[y/N]：');
          var confirm = stdin.readLineSync() ?? '';
          if (confirm.toLowerCase() != 'y') break;
          await Instance.get.article.reward(_currentDetail.oId).then((value) {
            page(':page article ${_currentDetail.oId}');
          }).catchError((err) {
            print('打赏失败：$err');
          });
        }
      case ':help':
        {
          print('''${Command.bold}文章模块命令${Command.restore}
:page article [page] [type] 查看文章，page 为页码，type 为文章类型
:type <type> 查看某个类型的文章
:tag <tag> 查看某个 Tag 下的文章
:to <page> 跳转到(文章或评论)某一页
:all 查看所有文章（清除 Tag）
:next 下一页
:prev 上一页
:view <index|id> 查看某一篇文章
:reward 打赏当前文章
:reply <index> <content> 回复某一条评论
<content> 发送正在查看的文章评论 (Windows 不支持此命令)
''');
          break;
        }
      default:
        {
          if (_currentPage != ArticlePage.detail) break;

          if (Platform.isWindows) {
            print('命令发送评论不支援 Windows 端。');
            break;
          }

          if (!Instance.get.isLogin) {
            print('请先登录。');
            break;
          }

          await comment(CommentPost(
            articleId: _currentDetail.oId,
            content: command,
          ));
        }
    }
    return true;
  }

  comment(CommentPost comment) {
    return Instance.get.comment.send(comment).then((value) => {
          _commentPage = (_currentDetail.pagination?.paginationPageCount ?? 1) +
              (_currentDetail.articleComments.length == 30 ? 1 : 0),
          page(':page article ${_currentDetail.oId}')
        });
  }

  @override
  Future<bool> page(String command) async {
    try {
      print('${Command.clearScreen}${Command.moveTo(0, 0)}');
      int page = _page;
      String tag = _tag;
      String type = _type;
      final commands = command.trim().split(' ');
      if (commands.length > 2 &&
          commands[2].isNotEmpty &&
          ArticleListType.values.contains(commands[2])) {
        type = commands[2];
      } else if (commands.length > 2 && commands[2].length == 13) {
        if (commands[2] != _currentDetail.oId) {
          _commentPage = 1;
        }
        await Instance.get.article
            .detail(commands[2], p: _commentPage)
            .then((value) {
          _currentDetail = value;
          _currentPage = ArticlePage.detail;
          print(
              '${Command.bold}${_currentDetail.articleTitleEmoj}${Command.restore}');
          print(
              '${Command.from('#555555').color}👤 ${_currentDetail.articleAuthor.name} | 👀  ${_currentDetail.articleHeat} | 👍  ${_currentDetail.articleGoodCnt} | ❤️  ${_currentDetail.articleThankCnt} ${Command.restore}');
          print(htmlToText(_currentDetail.articleContent));
          if (_currentDetail.articleRewardContent.isNotEmpty) {
            print('''
${Command.from('#232425').back}🎁 ${_currentDetail.articleRewardPoint} (x${_currentDetail.rewardedCnt})
${_currentDetail.rewarded ? _currentDetail.articleRewardContent : '${Command.italic}尚未打赏'} ${Command.restore}
            ''');
          }
          print(
              '------ 评论 ($_commentPage / ${_currentDetail.pagination?.paginationPageCount ?? 1}) ------');
          for (var i = 0; i < _currentDetail.articleComments.length; i++) {
            var item = _currentDetail.articleComments[i];
            print(
                '${Command.from('#AAAAAA').color}{${i + 1}} ${Command.from('#888888').color}${Command.bold}${item.commenter.name}${Command.restore} ${Command.from('#555555').color}[${item.commentCreateTimeStr}]${Command.restore}: ${htmlToText(item.commentContent)}');
          }
        }).catchError((error) {
          print('找不到对应编号的文章');
          _currentPage = ArticlePage.list;
        });
        return true;
      } else if (commands.length > 2 &&
          RegExp(r'^\d+$').hasMatch(commands[2])) {
        page = int.parse(commands[2]);
      }

      print('------ <$_type> 文章列表 ${_tag.isEmpty ? '' : '[$_tag]'}------');

      await Instance.get.article
          .list(type: type, page: page, tag: tag)
          .then((list) {
        for (var i = 0; i < list.articles.length; i++) {
          var item = list.articles[i];
          print(
              '${(i + 1).toString().padLeft(2, '0')}.${Command.bold}${Command.from('#555555').color}[${item.articleAuthor.name}]${Command.restore} ${item.articleTitleEmoj}${Command.from('#222222').back}${Command.from('#a1e999').color} ${item.articleHeat} ${Command.restore}');
        }
        print('第 $page / ${list.pagination.paginationPageCount} 页');
        _current = list;
      });
    } catch (e) {
      print('未知异常：$e');
    }

    return true;
  }
}
