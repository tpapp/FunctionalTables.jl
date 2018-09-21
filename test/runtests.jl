using DataColumns, Test
using DataColumns: append1

include("utilities.jl")

@testset "append1" begin
    a1 = append1(ones(Int8, 2), missing)
    @test eltype(a1) ≡ Union{Missing, Int8}
    @test [1, 1, missing] ≅ a1
end


@testset "collect by names" begin
    itr = [(a = i, b = Float64(i), c = 'a' + i - 1) for i in 1:10]
    result = collect_sinks(SinkConfig(;useRLE = false), itr)
    @test result isa NamedTuple{(:a, :b, :c)}
    @test all(result.a .≡ 1:10)
    @test all(result.b .≡ Float64.(1:10))
    @test all(result.c .≡ (0:9) .+ 'a')
end

@testset "simple RLE" begin
    v = vcat(fill(1, 10), fill(missing, 5), fill(2, 20))
    s = collect_sink(SINKCONFIG, v)
    @test length(s) == 35
    @test eltype(s) ≡ Union{Int64, Missing}
    @test s.data == [1, 2]
    @test s.counts == [10, -5, 20]
    @test collect(s) ≅ v
end

@testset "overrun RLE" begin
    v = vcat(fill(1, 300), fill(missing, 5), fill(2, 20), fill(missing, 200))
    s = collect_sink(SINKCONFIG, v)
    @test length(s) == length(v)
    @test eltype(s) ≡ Union{Int64, Missing}
    @test s.data == [1, 1, 1, 2]
    @test s.counts == [127, 127, 300-(2*127), -5, 20, -128, -200+128]
    @test collect(s) ≅ v
end

@testset "large collection" begin
    v = randvector(1000)
    columns = collect_sinks(SINKCONFIG, [(a = a, ) for a in v])
    @test collect(columns.a) ≅ v
end
