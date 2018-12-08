#####
##### Informal benchmarks
#####

using FunctionalTables

N = 10000
a = rand(1:100, N)
b = rand('a':'z', N)
c = rand(Float64, N)

ft = sort(FunctionalTable((a = a, b = b, c = c)), (:a, :b))

m(ft) = map((_, ft) -> ft, by(ft, (:a, :b)))

@code_warntype m(ft)
@time m(ft)
# f77a599: 0.215s
# 0.09 after by optimization

f(ft) = iterate(by(ft, (:a, :b)))
@code_warntype f(ft)
