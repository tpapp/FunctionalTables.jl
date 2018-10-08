using FunctionalTables, Test
using FunctionalTables: cancontain, narrow, append1, NamedTupleSplitter # utilities

include("utilities.jl")

@testset "narrow" begin
    @test narrow(1) ≡ true
    @test narrow(-9) ≡ Int8(-9)
    @test narrow(128) ≡ Int16(128)
    @test narrow(2^17) ≡ Int32(2^17)
    @test narrow(2^32) ≡ Int(2^32)
end

@testset "cancontain" begin
    @test cancontain(Int8, -7)
    @test !cancontain(Int8, typemax(Int8) + 1)
    @test !cancontain(String, -7)
    @test cancontain(Float64, Int8(9))
    @test cancontain(Float64, maxintfloat(Float64))
    @test !cancontain(Float64, Int(maxintfloat(Float64)) + 1)
    for T in (Float32, Float64)
        # Test around `maxintfloat`, as it will fail if the number can be contained by accident.
        for elt in vcat(Int(maxintfloat(T)) .+ (-1:1), -Int(maxintfloat(T)) .+ (-1:1))
            v = T[]
            push!(v, elt)
            (oftype(elt, v[1]) ≡ elt) ≡ cancontain(T, elt)
        end
    end
end

@testset "append1" begin
    a1 = append1(ones(Int8, 2), missing)
    @test eltype(a1) ≡ Union{Missing, Int8}
    @test [1, 1, missing] ≅ a1
end

@testset "collect by names" begin
    itr = [(a = i, b = Float64(i), c = 'a' + i - 1) for i in 1:10]
    result = collect_columns(SinkConfig(;useRLE = false), itr)
    @test result isa NamedTuple{(:a, :b, :c), Tuple{Vector{Int8}, Vector{Float64}, Vector{Char}}}
    @test result.a ≅ 1:10
    @test result.b ≅ Float64.(1:10)
    @test result.c ≅ (0:9) .+ 'a'
end

@testset "simple RLE" begin
    v = vcat(fill(1, 10), fill(missing, 5), fill(2, 20))
    s = collect_column(SINKCONFIG, v)
    @test length(s) == 35
    @test eltype(s) ≡ Union{Int8, Missing}
    @test s.data == [1, 2]
    @test s.counts == [10, -5, 20]
    @test collect(s) ≅ v
end

@testset "overrun RLE" begin
    v = vcat(fill(1, 300), fill(missing, 5), fill(2, 20), fill(missing, 200))
    s = collect_column(SINKCONFIG, v)
    @test length(s) == length(v)
    @test eltype(s) ≡ Union{Int8, Missing}
    @test s.data == [1, 1, 1, 2]
    @test s.counts == [127, 127, 300-(2*127), -5, 20, -128, -200+128]
    @test collect(s) ≅ v
end

@testset "large collection" begin
    v = randvector(1000)
    columns = collect_columns(SINKCONFIG, [(a = a, ) for a in v])
    @test collect(columns.a) ≅ v
end

@testset "contiguous blocks" begin
    keycounts = [:a => 10, :b => 17, :c => 19]
    v = mapreduce(((k, c), ) -> [(k, (elt = i,)) for i in 1:c], vcat, keycounts)
    b = contiguous_blocks(identity, v)
    @test collect(b) == map(((k, c),) -> k => (elt = 1:c, ), keycounts)
end

@testset "splitting named tuples" begin
    s = NamedTupleSplitter((:a, :c))
    @test s((a = 1, b = 2, c = 3, d = 4)) ≡ ((a = 1, c = 3), (b = 2, d = 4))
    @test s((c = 1, b = 2, a = 3, d = 4)) ≡ ((a = 3, c = 1), (b = 2, d = 4))
    @test_throws ErrorException s((a = 1, b = 2))
end
