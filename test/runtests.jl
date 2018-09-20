using DataColumns
using Test

@testset "collect by names" begin
    itr = [(a = i, b = Float64(i), c = 'a' + i - 1) for i in 1:10]
    result = collect_by_names(SINKCONFIG, itr)
    @test result isa NamedTuple{(:a, :b, :c)}
    @test all(result.a .≡ 1:10)
    @test all(result.b .≡ Float64.(1:10))
    @test all(result.c .≡ (0:9) .+ 'a')
end
