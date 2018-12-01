export FunctionalTable, columns, ordering, select, head

struct FunctionalTable{C <: NamedTuple, O <: TableOrdering}
    len::Int
    columns::C
    ordering::O
    function FunctionalTable(columns::C, ordering_rule::R
                             ) where {C <: NamedTuple, R <: TrustOrdering}
        @unpack ordering = ordering_rule
        @argcheck !isempty(columns) "At least one column is needed."
        len = length(first(columns))
        @argcheck all(column -> length(column) == len, Base.tail(values(columns)))
        checkvalidkeys(orderkey.(ordering), keys(columns))
        new{C, typeof(ordering)}(len, columns, ordering)
    end
end

"""
$(SIGNATURES)

Return the columns in a `NamedTuple`.

Each column is an iterable, but not necessarily an `<: AbstractVector`.

!!! note
    **Never mutate columns obtained by this method**, as that will violate invariants
    assumed by the implementation. Use `map(collect, columns(ft))` or similar to obtain
    mutable vectors.
"""
columns(ft::FunctionalTable) = ft.columns

"""
$(SIGNATURES)

Return the ordering of the table, which is a tuple of `ColumnOrdering` objects.
"""
ordering(ft::FunctionalTable) = ft.ordering

function FunctionalTable(ft::FunctionalTable,
                         ordering_rule::OrderingRule{K} = VerifyOrdering()) where K
    @unpack ordering = ordering_rule
    K ≡ :trust && return FunctionalTable(ft.columns, ordering_rule)
    rule = is_prefix(ordering, ft.ordering) ? TrustOrdering(ordering) : ordering_rule
    FunctionalTable(ft.columns, rule)
end

function FunctionalTable(columns::NamedTuple,
                         ordering_rule::VerifyOrdering = VerifyOrdering(()))
    ft = FunctionalTable(columns, TrustOrdering(ordering_rule))
    @argcheck issorted(ft; lt = (a, b) -> isless_ordering(ft.ordering, a, b))
    ft
end

function FunctionalTable(columns::NamedTuple, ::TryOrdering)
    error("not implemented yet, maybe open an issue?")
end

Base.IteratorSize(::FunctionalTable) = Base.HasLength()

Base.length(ft::FunctionalTable) = ft.len

Base.IteratorEltype(::FunctionalTable) = Base.HasEltype()

Base.eltype(ft::FunctionalTable) =
    NamedTuple{keys(ft.columns), Tuple{map(eltype, values(ft.columns))...}}

"""
$(SIGNATURES)

Create a `FunctionalTable` from an iterable that returns `NamedTuple`s.

Returned values need to have the same names (but not necessarily types).

`ordering_rule` specifies sorting. The `VerifyOrdering` (default), `TrustOrdering`, and
`TryOrdering`  constructors take a tuple of a tuple of `:key` or `:key => reverse` elements.

`cfg` determines sink configuration for collecting elements of the columns, see
[`SinkConfig`](@ref).
"""
function FunctionalTable(itr, ordering_rule::OrderingRule = TrustOrdering();
                         cfg::SinkConfig = SINKCONFIG)
    FunctionalTable(collect_columns(cfg, itr, ordering_rule)...)
end

function Base.iterate(ft::FunctionalTable, states...)
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

function Base.show(io::IO, ft::FunctionalTable)
    @unpack len, columns, ordering = ft
    print(io, "FunctionalTable of $(len) rows, ", ordering_repr(ordering))
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
    FunctionalTable(NamedTuple{keep}(ft.columns),
                    TrustOrdering(select_ordering(ft.ordering, keep)))
end

select(ft::FunctionalTable, keep::Symbol...) = select(ft, keep)

select(ft::FunctionalTable; drop::Keys) = select(ft, dropkeys(keys(ft.columns), drop))

"""
$(SIGNATURES)

Merge two `FunctionalTable`s.

When `replace == true`, columns in the first one are replaced by second one, otherwise an
error is thrown if column names overlap.
"""
function Base.merge(a::FunctionalTable, b::FunctionalTable; replace = false)
    @argcheck length(a) == length(b)
    if !replace
        dup = tuple((keys(a.columns) ∩ keys(b.columns))...)
        @argcheck isempty(dup) "Duplicate columns $(dup). Use `replace = true`."
    end
    FunctionalTable(merge(a.columns, b.columns),
                    TrustOrdering(merge_ordering(a.ordering, keys(b.columns))))
end

"""
$(SIGNATURES)
"""
Base.map(f, ft::FunctionalTable; cfg = SINKCONFIG) = FunctionalTable(imap(f, ft); cfg = cfg)

"""
$(SIGNATURES)

Map `ft` using `f` by rows, then `merge` the two. See [`map(f, ::FunctionalTable)`](@ref).

`cfg` is passed to `map`, `replace` governs replacement of overlapping columns in `merge`.
"""
function Base.merge(f, ft::FunctionalTable; cfg = SINKCONFIG, replace = false)
    merge(ft, map(f, ft; cfg = cfg); replace = replace)
end

Base.filter(f, ft::FunctionalTable; cfg = SINKCONFIG) =
    FunctionalTable(Iterators.filter(f, ft), TrustOrdering(ft.ordering))

"""
$(SIGNATURES)

A `FunctionalTable` of the first `n` rows. For previews etc.
"""
head(ft::FunctionalTable, n::Integer) =
    FunctionalTable(Iterators.take(ft, n), TrustOrdering(ft.ordering))
