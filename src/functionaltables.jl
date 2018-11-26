export FunctionalTable, columns, select

struct FunctionalTable{C <: NamedTuple, S <: Sorting}
    len::Int
    columns::C
    sorting::S
    function FunctionalTable(columns::C; sorting::Tuple = ()) where {C <: NamedTuple}
        @argcheck !isempty(columns) "At least one column is needed."
        sorting = sorting_sortspecs(sorting, keys(columns))
        len = length(first(columns))
        @argcheck all(column -> length(column) == len, Base.tail(values(columns))) #
        new{C, typeof(sorting)}(len, columns, sorting)
    end
end

keys(ft::FunctionalTable) = keys(ft.columns)

validkeys(keys_::Keys, ft::FunctionalTable) = validkeys(keys_, keys(ft))

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

Create a `FunctionalTable` from either

1. a `NamedTuple` of columns (checked for length),

2. an iterable that returns `NamedTuple`s with the same names (but not necessarily types).

`sorting` specifies sorting, and is a tuple of `:key` or `:key => reverse` elements.

`cfg` determines sink configuration for collecting elements of the columns, see
[`SinkConfig`](@ref).
"""
FunctionalTable(itr; sorting::Tuple = (), cfg::SinkConfig = SINKCONFIG) =
    FunctionalTable(collect_columns(cfg, itr); sorting = sorting)

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
    print(io, "FunctionalTable of $(len) rows, ")
    if isempty(sorting)
        print(io, "no sorting")
    else
        print(io, "sorted ")
        for (i, cs) in enumerate(sorting)
            i > 1 && print(io, ",")
            print(io, cs)
        end
    end
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
select(ft::FunctionalTable, keep::Keys) =
    FunctionalTable(NamedTuple{keep}(ft.columns);
                    sorting = select_sorting(ft.sorting, keep))

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
    FunctionalTable(merge(a.columns, b.columns); sorting = merge_sorting(a.sorting, keys(b)))
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
