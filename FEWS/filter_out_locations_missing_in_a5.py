import pandas

for v in ["P", "H", "Q"]:
    series_a5 = pandas.read_csv(open("results/INA_%s_all.csv" % v,"r"))
    series_whos = pandas.read_csv(open("results/ina/%s.csv" % v,"r"))
    series_whos_in_a5 = series_whos[series_whos["PARENT_ID"].isin(series_a5["PARENT_ID"].unique())]
    f = open("results/ina/%s_in_a5.csv" % v,"w")
    f.write(series_whos_in_a5.to_csv(index=False))
    f.close()

