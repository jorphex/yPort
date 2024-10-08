### yPort
Discord and Telegram bots that help Yearn users monitor their Yearn Vault deposits across multiple addresses by generating detailed portfolio reports including yield forecasting, APR tracking, and personalized vault suggestions based on users' current Vault deposits. It provides users with consolidated information about how their Vaults are performing and suggests Vaults with higher APRs when applicable.

The bots do not require users to connect their wallets or interact with the blockchain. It simply reports on existing Vaults and provides information based on publicly available Vault data.

### Telegram
Start by sending /start, then submit your addresses or ENS names when prompted. The bot will process your addresses and send an immediate report. You’ll also receive daily reports at 00:00 UTC. You can request a report anytime using /yport. Seven-day and thirty-day yield estimates are included in the reports. Along with the report, the bot also sends vault suggestions with higher APRs of at least 3%, if available, based on the underlying assets of your current Vault deposits.

The bot is available for public use on Telegram here: [@yPort](https://t.me/yPortBot)
### Discord
Start by sending your addresses or ENS names to the bot by DM. The bot will process your addresses and reply with a confirmation with a link to the specified public channel. In the public channel, send /yport to request a report at any time. Seven-day and thirty-day yield estimates are included in the reports. Along with the report, the bot also sends vault suggestions with higher APRs of at least 3%, if available, based on the underlying assets of your current Vault deposits. To ensure privacy, these report messages are sent using Discord ephemeral messages feature, meaning only the user that triggers the command will see the individual requested report in the public channel. At 0000 and 1200 UTC, the bot sends a public report containing the top five single-asset vaults by APR, while also deleting the previous 12-hour report to avoid flooding the channel. 

The bot is available for public use in the Yearn Discord here: [#yPort](https://discord.com/channels/734804446353031319/1279431421760507976)
### ⏳
To reduce unnecessary API requests, please avoid spamming /yport.
