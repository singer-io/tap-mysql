# Changelog

## 1.17.5
  * Revert #122, bump pymysql-replication to 0.22  [#133](https://github.com/singer-io/tap-mysql/pull/133)

## 1.17.4
  * Fix bookmarking of binlog to avoid skipping deletes [#115](https://github.com/singer-io/tap-mysql/pull/115)

## 1.17.3
  * Fix for python-mysql-replication bug where large json values cause errors [#122](https://github.com/singer-io/tap-mysql/pull/122)

## 1.17.2
  * Fix bug with JSON columns in binlog [#120](https://github.com/singer-io/tap-mysql/pull/120)

## 1.17.1
  * Fix monkey patching bug [#118](https://github.com/singer-io/tap-mysql/pull/118)

## 1.17.0
  * Bump PyMySQL dependency from 0.7.11 to 0.9.3 [#116](https://github.com/singer-io/tap-mysql/pull/116)
