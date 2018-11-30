export FunctionalTable, columns, select

struct FunctionalTable{C <: NamedTuple, S <: ColumnSorting}
    len::Int
    columns::C
    sorting::S
    function FunctionalTable(columns::C, sorting::S, ::SortingPolicy{:trust}
                             ) where {C <: NamedTuple, S <: ColumnSorting}
        @argcheck !isempty(columns) "At least one column is needed."
        len = length(first(columns))
        @argcheck all(column -> length(column) == len, Base.tail(values(columns)))
        checkvalidkeys(keys(sorting), keys(columns))
        new{C, S}(len, columns, sorting)
    end
end

function FunctionalTable(columns::NamedTuple, sorting::ColumnSorting, ::SortingPolicy{:verify})
    ft = FunctionalTable(columns, sorting, SORTING_TRUST)
    @argcheck issorted(ft; lt = (a, b) -> isless_sorting(sorting, a, b))
    ft
end

function FunctionalTable(columns::NamedTuple, sorting::ColumnSorting, ::SortingPolicy{:try})
    error("not implemented yet, maybe open an issue?")
end

FunctionalTable(columns::NamedTuple, sortspecs = (),
                sortingpolicy::SortingPolicy = SORTING_VERIFY) =
    FunctionalTable(columns, column_sorting(sortspecs, keys(columns)), sortingpolicy)

keys(ft::FunctionalTable) = keys(ft.columns)

IteratorSize(::FunctionalTable) = Base.HasLength()

length(ft::FunctionalTable) = ft.len

IteratorEltype(::FunctionalTable) = Base.HasEltype()

eltype(ft::FunctionalTable) = NamedTuple{keys(ft), Tuple{map(eltype, values(ft.columns))...}}

getsorting(ft::FunctionalTable) = ft.sorting

"""
$(SIGNATURES)

Return the columns in a `NamedTuple`.

When `mutable`, all columns will be mutable `<: AbstractVector`, and not share (shallow)
structure.

When `vector`, all columns will be `<: AbstractVector`, but may be immutable or share
structure.
"""
function columns(ft::FunctionalTable; vector = false, mutable = false)
    map(ft.columns) do c
        if c isa AbstractVector
            mutable ? collect(c) : c
        else
            (vector | mutable) ? collect(c) : c
        end
    end
end


"""
$(SIGNATURES)

Create a `FunctionalTable` from an iterable that returns `NamedTuple`s.

Returned values need to have the same names (but not necessarily types).

`sorting` specifies sorting, and is a tuple of `:key` or `:key => reverse` elements.

`cfg` determines sink configuration for collecting elements of the columns, see
[`SinkConfig`](@ref).
"""
function FunctionalTable(itr, sortspecs = (), sortingpolicy::SortingPolicy = SORTING_VERIFY;
                         cfg::SinkConfig = SINKCONFIG)
    FunctionalTable(collect_columns(cfg, itr, column_sorting(sortspecs), sortingpolicy)...,
                    SORTING_TRUST)
end

function iterate(ft::FunctionalTable, states...)
    ys = map(iterate, ft.columns, states...)
    any(isequal(nothing), ys) && return nothing
    map(first, ys), map(last, ys)
end

"""
Shows this many values from each column in a `FunctionalTable`.
"""
const SHOWROWS = 5

function _showcolcontents(io::IO, itr)
    elts = collect(Iterators.take(itr, SHOWROWS + 1))
    print(io, eltype(itr), "[")
    for (i, elt) in enumerate(elts)
        i > 1 && print(io, ", ")
        i > SHOWROWS ? print(io, "…") : show(io, elt)
    end
    print(io, "]")
end

function show(io::IO, ft::FunctionalTable)
    @unpack len, columns, sorting = ft
    print(io, "FunctionalTable of $(len) rows, ", sorting)
    ioc = IOContext(io, :compact => true)
    for (key, col) in pairs(ft.columns)
        println(ioc)
        print(ioc, "    ", key, " = ")
        _showcolcontents(ioc, col)
    end
end

"""
$(SIGNATURES)
select(ft, keep...)
select(ft; drop)

Select a subset of columns from the table.

`select(ft, keep)` and `select(ft, keep...)` returns the table with the given columns.

`select(ft; drop = keys)` is a convenience form for keeping **all but** the given columns.
"""
function select(ft::FunctionalTable, keep::Keys)
    FunctionalTable(NamedTuple{keep}(ft.columns), select_sorting(ft.sorting, keep),
                    SORTING_TRUST)
end

select(ft::FunctionalTable, keep::Symbol...) = select(ft, keep)

select(ft::FunctionalTable; drop::Keys) = select(ft, dropkeys(keys(ft), drop))

"""
$(SIGNATURES)

Merge two `FunctionalTable`s.

When `replace == true`, columns in the first one are replaced by second one, otherwise an
error is thrown if column names overlap.
"""
function merge(a::FunctionalTable, b::FunctionalTable; replace = false)
    @argcheck length(a) == length(b)
    if !replace
        dup = tuple((keys(a) ∩ keys(b))...)
        @argcheck isempty(dup) "Duplicate columns $(dup). Use `replace = true`."
    end
    FunctionalTable(merge(a.columns, b.columns), merge_sorting(a.sorting, keys(b)),
                    SORTING_TRUST)
end

"""
$(SIGNATURES)
"""
map(f::Callable, ft::FunctionalTable; cfg = SINKCONFIG) =
    FunctionalTable(imap(f, ft); cfg = cfg)

"""
$(SIGNATURES)

Map `ft` using `f` by rows, then `merge` the two. See
[`map(::Callable,::FunctionalTable)`](@ref).

`cfg` is passed to `map`, `replace` governs replacement of overlapping columns in `merge`.
"""
merge(ft::FunctionalTable, f::Callable; cfg = SINKCONFIG, replace = false) =
    merge(ft, map(f, ft; cfg = cfg); replace = replace)

filter(f, ft::FunctionalTable; cfg = SINKCONFIG) =
    FunctionalTable(Iterators.filter(f, ft), getsorting(ft), SORTING_TRUST)
