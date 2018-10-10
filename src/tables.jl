export FunctionalTable, colselect, coldrop

"""
$(TYPEDEF)

Sort specification for a column.

Not part of the interface, for internal representation.
"""
struct ColumnSort
    key::Symbol
    rev::Bool
end

"""
$(SIGNATURES)

Process an individual sort specification, called by [`column_sorting`](@ref).
"""
column_sort(key::Symbol) = ColumnSort(key, false)
column_sort(keyrev::Pair{Symbol, typeof(reverse)}) = ColumnSort(first(keyrev), true)
column_sort(cs::ColumnSort) = cs
column_sort(x) = throw(ArgumentError("Unrecognized sorting specification $(x)."))

"""
$(SIGNATURES)

Process sorting specifications for columns.

Verify that sort keys are unique. When `keys` is given, verify that the sort keys are a
subset of it.

Accepted syntax:

- `:key`, for sorting a column in ascending order

- `:key => reverse`, for sorting a column in descending order

All functions which accept sort specs should use this, but the function itself is not part
of the API.
"""
function column_sorting(sortspecs, keys::Union{Nothing,Tuple{Vararg{Symbol}}} = nothing)
    sorting = map(column_sort, tuple(sortspecs...))
    sortkeys = map(c -> c.key, sorting)
    @argcheck allunique(sortkeys) "Duplicate sort keys."
    keys ≡ nothing || @argcheck sortkeys ⊆ keys
    sorting
end

struct FunctionalTable{C <: NamedTuple, S <: Tuple{Vararg{ColumnSort}}}
    len::Int
    columns::C
    sorting::S
    function FunctionalTable(columns::C; sortspecs::Tuple = ()) where {C <: NamedTuple}
        @argcheck !isempty(columns) "At least one column is needed."
        sorting = column_sorting(sortspecs, keys(columns))
        len = length(first(columns))
        @argcheck all(column -> length(column) == len, Base.tail(values(columns))) #
        new{C, typeof(sorting)}(len, columns, sorting)
    end
end

keys(ft::FunctionalTable) = keys(ft.columns)

IteratorSize(::FunctionalTable) = Base.HasLength()

length(ft::FunctionalTable) = ft.len

IteratorEltype(::FunctionalTable) = Base.HasEltype()

eltype(ft::FunctionalTable) = NamedTuple{keys(ft), Tuple{map(eltype, values(ft.columns))...}}

"""
$(SIGNATURES)

Create a `FunctionalTable` from either

1. a `NamedTuple` of columns (checked for length),

2. an iterable that returns `NamedTuple`s with the same names (but not necessarily types).

`sortspecs` specifies sorting, and is a tuple of `:key` or `:key => reverse` elements.

`cfg` determines sink configuration for collecting elements of the columns, see
[`SinkConfig`](@ref).
"""
FunctionalTable(itr; sortspecs::Tuple = (), cfg::SinkConfig = SINKCONFIG) =
    FunctionalTable(collect_columns(cfg, itr); sortspecs = sortspecs)

function iterate(ft::FunctionalTable, states...)
    ys = map(iterate, ft.columns, states...)
    any(isequal(nothing), ys) && return nothing
    map(first, ys), map(last, ys)
end

"""
$(SIGNATURES)

The table with only the specified columns.
"""
function colselect(ft::FunctionalTable, keep::Tuple{Vararg{Symbol}})
    FunctionalTable(NamedTuple{keep}(ft.columns);
                    sortspecs = tuple(filter(cs -> cs.key ∈ keep, [ft.sorting...])...))
end

colselect(ft::FunctionalTable, keep::Symbol...) = colselect(ft, keep)

"""
$(SIGNATURES)

The table without the specified columns.
"""
function coldrop(ft::FunctionalTable, drop::Tuple{Vararg{Symbol}})
    ftkeys = keys(ft)
    @assert drop ⊆ ftkeys "Cannot drop keys which are not in the table."
    colselect(ft, tuple(setdiff(ftkeys, drop)...))
end

coldrop(ft::FunctionalTable, drop::Symbol...) = coldrop(ft, drop)

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
            print(io, cs.rev ? "↓" : "↑", cs.key)
        end
    end
    ioc = IOContext(io, :compact => true)
    for (key, col) in pairs(ft.columns)
        println(ioc)
        print(ioc, "    ", key, " = ")
        _showcolcontents(ioc, col)
    end
end
