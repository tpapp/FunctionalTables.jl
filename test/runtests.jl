using FunctionalTables, Test
using FunctionalTables:
    # utilities
    cancontain, narrow, append1, split_namedtuple, merge_sorting,
    # column sorting building blocks
    ColumnSort, column_sorting, cmp_sorting, retained_sorting
import Tables

include("utilities.jl")         # utilities for tests

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
        # Test around `maxintfloat`, as it will fail if the number can be contained by
        # accident.
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
    sorting = column_sorting((:a, :b, :c))
    result, s = collect_columns(SinkConfig(;useRLE = false), itr, sorting, SORTING_TRUST)
    @test result isa NamedTuple{(:a, :b, :c), Tuple{Vector{Int8}, Vector{Float64}, Vector{Char}}}
    @test result.a ≅ 1:10
    @test result.b ≅ Float64.(1:10)
    @test result.c ≅ (0:9) .+ 'a'
    @test s ≡ sorting
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
    columns, sorting = collect_columns(SINKCONFIG, [(a = a, ) for a in v],
                                       column_sorting(()), SORTING_TRUST)
    @test collect(columns.a) ≅ v
    @test sorting ≡ column_sorting(())
end

@testset "splitting named tuples" begin
    s = NamedTuple{(:a, :c)}
    @test split_namedtuple(s, (a = 1, b = 2, c = 3, d = 4)) ≡ ((a = 1, c = 3), (b = 2, d = 4))
    @test split_namedtuple(s ,(c = 1, b = 2, a = 3, d = 4)) ≡ ((a = 3, c = 1), (b = 2, d = 4))
    @test_throws ErrorException split_namedtuple(s, (a = 1, b = 2))
end

@testset "column sorting specifications" begin
    @test column_sorting((:a, :b => reverse, ColumnSort(:c, false))) ==
        FunctionalTables.ColumnSorting((ColumnSort(:a, false), ColumnSort(:b, true),
                                        ColumnSort(:c, false)))
    @test_throws ArgumentError column_sorting(("foobar", "baz")) # invalid
    @test_throws ArgumentError column_sorting((:a, :a))          # duplicate
    @test_throws ArgumentError column_sorting((:a, :a), (:b, ))  # not in set

    @test repr(column_sorting(())) == "no sorting"
    @test repr(column_sorting((:a, :b => reverse))) == "sorting ↑a ↓b"
end

@testset "retained sorting" begin
    s = column_sorting((:a, :b => reverse))
    row = (a = 1, b = 2, c = 3)
    @test retained_sorting(s, row, row) ≡ s
    @test retained_sorting(s, row, (a = 2, b = 1, c = -1)) ≡ s
    @test retained_sorting(s, row, (a = 2, b = 3, c = -1)) ≡ column_sorting((:a, ))
    @test @inferred(retained_sorting(column_sorting(()), row, row)) ≡ column_sorting(())
end

@testset "FunctionalTable basics and column operations" begin
    A = 1:10
    B = 'a':('a'+9)
    C = Float64.(21:30)
    ft = FunctionalTable((a = A, b = B, c = C))
    @test Base.IteratorEltype(ft) ≡ Base.HasEltype()
    @test eltype(ft) ≡ typeof((a = first(A), b = first(B), c = first(C)))
    @test Base.IteratorSize(ft) ≡ Base.HasLength()
    @test length(ft) ≡ length(A)
    @test keys(ft) == (:a, :b, :c)
    @test select(ft, (:a, :b)) ≅ FunctionalTable((a = A, b = B)) ≅ select(ft, :a, :b)
    @test select(ft; drop = (:a, :b)) ≅ FunctionalTable((c = C,))
    @test FunctionalTable(ft) ≅ ft
    cols = columns(ft; mutable = true, vector = true)
    @test all(isa.(values(cols), AbstractVector))
    @test cols.a == A && cols.a ≢ A
    @test cols.b == B && cols.b ≢ B
    @test cols.c == C && cols.c ≢ C
end

@testset "merge sorting" begin
    s = column_sorting((:a, :b, :c))
    @test merge_sorting(s, (:d, :e)) ≡ s
    @test merge_sorting(s, (:c, :b)) ≡ column_sorting((:a, ))
    @test merge_sorting(s, (:a, :b, :c)) ≡ column_sorting(())
end

@testset "merging" begin
    A = 1:10
    B = 'a':('a'+9)
    C = Float64.(21:30)
    A2 = .-A
    ft = FunctionalTable((a = A, b = B), (:a, :b))
    @test merge(ft, FunctionalTable((c = C, ))) ≅
        FunctionalTable((a = A, b = B, c = C), (:a, :b))
    @test_throws ArgumentError merge(ft, FunctionalTable((c = C, a = A2)))
    @test merge(ft, FunctionalTable((c = C, a = A2)); replace = true) ≅
        FunctionalTable((a = A2, b = B, c = C), ())
end

@testset "map" begin
    A = 1:10
    B = 'a':('a'+9)
    ft = FunctionalTable((a = A, b = B), (:a, :b))
    f(row) = (b = row.a + 1, c = row.b + 2)
    B2 = A .+ 1
    C = collect(B .+ 2)
    ft2 = map(f, ft)
    # NOTE map removes sorting
    @test ft2 ≅ FunctionalTable((b = B2, c = C))
    ft3 = merge(ft, f; replace = true)
    # NOTE as :b is replaced, its sorting is removed
    @test ft3 ≅ FunctionalTable((a = A, b = B2, c = C), (:a, ))
    # overlap, without replacement
    @test_throws ArgumentError merge(ft, f)
end

@testset "filter" begin
    A = 1:5
    B = 'a':'e'
    s = (:a, :b)
    ft = FunctionalTable((a = A, b = B), s)
    @test filter(row -> isodd(row.a), ft) ≅
        FunctionalTable((a = [1, 3, 5], b = ['a', 'c', 'e']), s)
end

@testset "groupby 1" begin
    keycounts = [:a => 10, :b => 17, :c => 19]
    ft = FunctionalTable(mapreduce(((k, c), ) -> [(sym = k, val = i)
                                                  for i in 1:c], vcat, keycounts),
                         (:sym, :val))
    g = by(ft, (:sym, ))
    cg = collect(g)
    for (i, (s, c)) in enumerate(keycounts)
        @test FunctionalTable(cg[i]) ≅ FunctionalTable((sym = fill(s, c), val = 1:c))
    end
end

@testset "groupby 2" begin
    A = [1, 1, 1, 2, 2]
    B = 'a':'e'
    ft = FunctionalTable((a = A, b = B), (:a, ))
    g = by(ft, (:a, ))
    @test Base.IteratorSize(g) ≡ Base.SizeUnknown()
    result = collect(g)
    @test result ≅ [FunctionalTable((a = GroupedColumn(1, 3), b = ['a', 'b', 'c'],)),
                    FunctionalTable((a = GroupedColumn(2, 2), b = ['d', 'e'],))]
end

@testset "tables interface" begin
    cols = (a = 1:10, b = 'a' .+ (1:10))
    ft = FunctionalTable(cols)
    # general sanity checks
    @test Tables.columntable(ft) == cols
    @test Tables.rowtable(ft) == Tables.rowtable(cols)
    # detailed API testing
    @test Tables.istable(ft)
    @test Tables.rowaccess(ft)
    @test Tables.schema(Tables.rows(ft)) == Tables.schema(Tables.rows(cols))
    @test Tables.columnaccess(ft)
    @test Tables.schema(Tables.columns(ft)) == Tables.schema(Tables.columns(cols))
end

@testset "column sort comparisons" begin
    sorting = column_sorting((:a, :b => reverse))
    @test @inferred cmp_sorting(sorting, (a = 1, b = 2), (a = 1, b = 2)) == 0
    @test @inferred cmp_sorting(sorting, (a = 1, b = 3), (a = 1, b = 2)) == -1
    @test @inferred cmp_sorting(sorting, (a = 0, b = 3), (a = 1, b = 2)) == -1
    @test @inferred cmp_sorting(sorting, (a = 1, b = 2), (b = 2, a = 1)) == 0 # order irrelevant
    @test_throws ErrorException cmp_sorting(sorting, (c = 1, ), (c = 1, ))    # no such field
end

@testset "sort" begin
    ft = FunctionalTable((a = [1, -1, 3, 1, 2],
                          b = [2, 2, 1, 2, 2],
                          c = 1:5))
    sft = sort(ft, (:b, :a => reverse))
    @test sft ≅ FunctionalTable((a = [3, 2, 1, 1, -1],
                                 b = [1, 2, 2, 2, 2],
                                 c = [3, 5, 1, 4, 2]),
                                (:b, :a => reverse))
end

@testset "grouping" begin
    a = [1, 1, 1, 2, 2]
    b = 1:5
    ft = FunctionalTable((a = a, b = b), (:a, :b))
    f(ft) = map(sum, columns(ft))
    @test by(Ref ∘ f, ft, (:a, )) ≅ FunctionalTable((a = [3, 4], b = [6, 9]), (:a, :b))

    ft = FunctionalTable((a = GroupedColumn(1, 3), b = 1:3, c = GroupedColumn(7, 3)))
    f(ft) = FunctionalTable(map(x -> x .+ 1, columns(ft)))
    @test FunctionalTables.groupedkeys(ft) ≡ (:a, :c)
    ft2 = map_nongrouped(f)(ft)
    @test columns(ft2) ≡ (a = GroupedColumn(1, 3), c = GroupedColumn(7, 3), b = 2:4)
    @test FunctionalTables.getsorting(ft2) ≡ column_sorting(())
end

@testset "corner cases for collecting and sorting" begin
    A = (a = 1, )
    AA = [A, A]

    # different keys by row
    @test_throws ArgumentError FunctionalTable([A, (a = 1, b = 2)])

    # field specified by sorting is missing
    @test_throws ErrorException FunctionalTable(AA, (:b, ))

    # prefix narrows sorting silently
    @test FunctionalTable(AA, (:b, ), SORTING_TRY) ≅ FunctionalTable(AA)

    # sorting keys not contained in columns
    @test_throws ArgumentError FunctionalTable((AA, (:b, ), SORTING_TRUST))
    @test_throws ArgumentError FunctionalTable((AA, (:b, ), SORTING_VERIFY))

    # FIXME not implemented yet
    @test_skip FunctionalTable((a = [2, 1], ), (:a, ), SORTING_TRY) ≅
        FunctionalTable((a = [1, 2], ), (), SORTING_TRUST)
end

@testset "printing" begin
    ft = FunctionalTable((a = [1, 2], b = [3, 4]), (:a, :b => reverse), SORTING_TRUST)
    reprft = """
    FunctionalTable of 2 rows, sorting ↑a ↓b
        a = Int64[1, 2]
        b = Int64[3, 4]"""
    @test repr(ft) == reprft
end
