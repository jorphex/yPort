yport

- use calculated 7d apr and apy from kong timeseries pps with fallback to ydaemon apr
  - add multiplot graphs for deposits and suggestions, and show underlying asset increase over time (token x pps) as bar graph or anotation on pps points
  - add note for vault deposits that have low apr variance for two weeks
- add pie chart of deposits allocation
- track daily yport reports, not just on demand use
- add earnings since deposit, USD (needs balanceOf and deposit tx)
- add command to track other vaults in a watchlist (and remove)
- use balanceOf and kong to calculate yield earned over the past 7 days
- add note if staking available
- add note if isRetired
- handle user blocked daily report messages (pause)