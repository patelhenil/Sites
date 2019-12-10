with open("FPARUN.csv") as input, open("FPA-ans.csv", 'wb') as output:
    non_blank = (line for line in input if line.strip())
    output.writelines(non_blank)
