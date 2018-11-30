#####
##### Building blocks for sorting.
#####
##### Actual sorting is implemented in sort.jl.
#####

export TrustSorting, TrySorting, VerifySorting

####
####
####

"""
$(TYPEDEF)

Sort specification for a column. `K::Symbol` is a key for sorting, `R::Bool` reverses
sorting for this key.

This type is *not part of the interface*, for internal representation.
"""
struct ColumnSort{K, R}
    function ColumnSort{K, R}() where {K, R}
        @argcheck K isa Symbol
        @argcheck R isa Bool
        new{K, R}()
    end
end

"""
$(SIGNATURES)

Accessor for sort key. *Internal.*
"""
sortkey(cs::ColumnSort{K}) where {K} = K

"""
$(SIGNATURES)

Process an individual sort specification, called by [`column_sorting`](@ref).
"""
@inline ColumnSort(key::Symbol, rev::Bool = false) = ColumnSort{key, rev}()
@inline ColumnSort(keyrev::Pair{Symbol, typeof(reverse)}) = ColumnSort(first(keyrev), true)
@inline ColumnSort(cs::ColumnSort) = cs
ColumnSort(x) = throw(ArgumentError("Unrecognized sorting specification $(x)."))

show(io::IO, cs::ColumnSort{K, R}) where {K, R} = print(io, R ? "↓" : "↑", K)

####
#### `ColumnSorting` type and interface
####

"""
Type for sorting, used internally.
"""
struct ColumnSorting{T <: Tuple{Vararg{ColumnSort}}}
    sorting::T
end

keys(cs::ColumnSorting) = sortkey.(cs.sorting)

function show(io::IO, cs::ColumnSorting)
    @unpack sorting = cs
    if isempty(sorting)
        print(io, "no sorting")
    else
        print(io, "sorting ")
        join(io, sorting, " ")
    end
end


"""
$(SIGNATURES)

Process sorting specifications for columns (an iterable or possibly a ColumnSorting), return
a value of type `ColumnSorting`.

Verify that sort keys are unique. When `colkeys` is given, verify that the sort keys are a
subset of it.

Accepted syntax:

- `:key`, for sorting a column in ascending order

- `:key => reverse`, for sorting a column in descending order

All functions which accept sort specs should use this, but the function itself is not part
of the API.
"""
function column_sorting(sortspecs, colkeys::Union{Nothing,Keys} = nothing)
    sorting = map(ColumnSort, tuple(sortspecs...))
    sortkeys = sortkey.(sorting)
    @argcheck allunique(sortkeys) "Duplicate sort keys."
    colkeys ≡ nothing || @argcheck sortkeys ⊆ colkeys
    ColumnSorting(sorting)
end

column_sorting(cs::ColumnSorting, colkeys::Union{Nothing, Keys}) =
    column_sorting(cs.sorting, colkeys)

"""
$(SIGNATURES)

Calculate sorting when a table with `sorting` is merged with a table containing `otherkeys`,
which may replace columns.
"""
function merge_sorting(cs::ColumnSorting, otherkeys::Keys)
    @unpack sorting = cs
    firstinvalid = findfirst(s -> sortkey(s) ∈ otherkeys, sorting)
    ColumnSorting(firstinvalid ≡ nothing ? sorting : sorting[1:(firstinvalid-1)])
end

"""
$(SIGNATURES)

Calculate sorting when only `keep` keys are kept.

`keep` may contain keys not in the sorting, ie those of a `FunctionalTable`.
"""
select_sorting(cs::ColumnSorting, keep::Keys) =
    ColumnSorting(_select_sorting(keep, cs.sorting...))

_select_sorting(keep) = ()

function _select_sorting(keep, cs, rest...)
    csrest = _select_sorting(keep, rest...)
    sortkey(cs) ∈ keep ? (cs, csrest...) : csrest
end

####
#### Comparisons
####

function cmp_columnsort(cs::ColumnSort{K, R}, a::NamedTuple, b::NamedTuple) where {K, R}
    cmp(getproperty(a, K), getproperty(b, K)) * (R ? -1 : 1)
end

"""
$(SIGNATURES)

Compare rows `a` and `b`, which support the `getproperty` interface, with the given column
sorting.

*Internal*.
"""
@inline cmp_sorting(cs::ColumnSorting, a, b) = _cmp_sorting(a, b, cs.sorting...)

_cmp_sorting(a, b) = 0

function _cmp_sorting(a, b, cs::ColumnSort, rest...)
    r = cmp_columnsort(cs, a, b)
    r ≠ 0 && return r
    _cmp_sorting(a, b, rest...)
end

@inline isless_sorting(cs::ColumnSorting, a, b) = cmp_sorting(cs, a, b) == -1

"""
$(SIGNATURES)

Return the part of `sorting` under which `a ≤ b`.
"""
retained_sorting(cs::ColumnSorting, a, b) = ColumnSorting(_retained_sorting(a, b, cs.sorting...))

_retained_sorting(a, b) = ()

function _retained_sorting(a, b, cs::ColumnSort, rest...)
    if cmp_columnsort(cs, a, b) ≤ 0
        (cs, _retained_sorting(a, b, rest...)...)
    else
        ()
    end
end

struct SortingPolicy{K}
    function SortingPolicy{K}() where K
        @argcheck K ∈ (:trust, :verify, :try)
        new{K}()
    end
end

const TrustSorting = SortingPolicy{:trust}

const VerifySorting = SortingPolicy{:verify}

const TrySorting = SortingPolicy{:try}
