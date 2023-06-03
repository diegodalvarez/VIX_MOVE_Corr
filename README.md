# VIX MOVE Disconect
Analyzing VIX / MOVE disconnect by including dispersion.

# Background 
Currently there is a disconnect between VIX and MOVE
![image](https://github.com/diegodalvarez/VIX_MOVE_Corr/assets/48641554/4aade782-f5e7-490b-b71d-6a4a0588bc8d)

We can compare the differences by using rolling z-scores
![image](https://github.com/diegodalvarez/VIX_MOVE_Corr/assets/48641554/e391441a-ce82-4b8d-a06b-8399b0313477)

If we look at distribution of differences
![image](https://github.com/diegodalvarez/VIX_MOVE_Corr/assets/48641554/c8167152-a487-46ca-bb7f-d4d5dccb68cc)

A consideration to make is that rolling z-scores is not suitable for financial assets since they assume normality and specifically for volatlity-related products. But in this case we will use them because we are measuring realtive value. We should also account that VIX and MOVE tend to have autoregressive properties.

# Motivation
A consideration is that the MOVE index is made with ATM Treasury options, while the VIX is made with ATM S&P 500 options. If we look under at the constituents we can see that the products in the Treasury universe are much more correlated with equities in the S&P 500 Universe. 
Treasury Correlation
![image](https://github.com/diegodalvarez/VIX_MOVE_Corr/assets/48641554/5d5b081e-f6fb-4a0f-949b-ac7c08e5fc17)
SPX Correlation
![image](https://github.com/diegodalvarez/VIX_MOVE_Corr/assets/48641554/cc4a31c5-161b-498c-be7f-dc0ef78cb43f)
Because there is likely to be less correlation (de-correlation) between equity names, there is a scenario where single-name volatilities' of S&P 500 exhibit more volatility than the index does. Take for example if 250 names moved up 1% while 250 names move down 1%, the index would not experience any movement (assuming equal weighting) while the single names will. 
