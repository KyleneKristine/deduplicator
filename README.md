# deduplicator
<i>Python script that queries Sierra's SQL database in order to find duplicate entries based on isbn.</i><br><br>
### Physical
This script will only find duplicates among non-electronic records. Currently, Sierra's Heading Reports will return an electronic and physical duplication which is unhelpful and clogs the cleanup process with false positives. To make it usable for our Acquistions department, I had all the printed information save to a text file so the script can be executed via an .exe file in the background while other work is done.
### DDA
This script is intended to prevent multiple purchases of Demand Driven Acquistions across our two DDA vendors. It searches for duplicates among records containing 910s of jstor, gobi, or ybp.
