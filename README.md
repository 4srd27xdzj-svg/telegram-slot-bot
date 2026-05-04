# Telegram slot watcher bot

Бот следит за Telegram-стикером игрового автомата `🎰` и отвечает на нужные комбинации.

## Подготовка

1. Создайте бота через [@BotFather](https://t.me/BotFather) и получите token.
2. Скопируйте `.env.example` в `.env`.
3. Вставьте token в `TELEGRAM_BOT_TOKEN`.
4. Настройте:
   - `JACKPOT_REPLY_TEXT` - ответ бота на `777`.
   - `THREE_OF_KIND_REPLY_TEXT` - ответ на три BAR, три лимона или три вишни/винограда.
   - `TWO_SEVENS_REPLY_TEXT` - ответ на первые две `7` и любой третий символ, кроме `7`.

## Запуск

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python bot.py
```

## Важно для групповых чатов

Если бот должен видеть все сообщения в группе, отключите privacy mode у бота:

1. Откройте [@BotFather](https://t.me/BotFather).
2. Выполните `/setprivacy`.
3. Выберите своего бота.
4. Выберите `Disable`.

После этого добавьте бота в нужный чат.

## Как это работает

Telegram отправляет игровой автомат как `dice`-сообщение. Для emoji `🎰` комбинации приходят числом от `1` до `64`.

```text
1  = BAR BAR BAR
22 = вишни/виноград вишни/виноград вишни/виноград
43 = лимон лимон лимон
16, 32, 48 = 7 7 любой другой символ
64 = 7 7 7
```
