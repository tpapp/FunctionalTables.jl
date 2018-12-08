#####
##### Informal benchmarks
#####

using FunctionalTables

N = 10000
a = rand(1:100, N)
b = rand('a':'z', N)
c = rand(Float64, N)

ft = sort(FunctionalTable((a = a, b = b, c = c)), (:a, :b))

@time map((_, ft) -> ft, by(ft, (:a, :b)));
# f77a599: 0.215s

@code_warntype iterate(by(ft, (:a, :b)))
