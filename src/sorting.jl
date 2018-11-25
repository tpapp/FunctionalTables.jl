#####
##### Building blocks for sorting.
#####
##### Actual sorting is implemented in sort.jl.
#####

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

Process an individual sort specification, called by [`sorting_sortspecs`](@ref).
"""
@inline ColumnSort(key::Symbol, rev::Bool = false) = ColumnSort{key, rev}()
@inline ColumnSort(keyrev::Pair{Symbol, typeof(reverse)}) = ColumnSort(first(keyrev), true)
@inline ColumnSort(cs::ColumnSort) = cs
ColumnSort(x) = throw(ArgumentError("Unrecognized sorting specification $(x)."))

####
#### `Sorting` type and interface
####

"""
Type for sorting, used internally.
"""
const Sorting = Tuple{Vararg{ColumnSort}}

"""
$(SIGNATURES)

Process sorting specifications for columns, return a value of type `Sorting`.

Verify that sort keys are unique. When `colkeys` is given, verify that the sort keys are a
subset of it.

Accepted syntax:

- `:key`, for sorting a column in ascending order

- `:key => reverse`, for sorting a column in descending order

All functions which accept sort specs should use this, but the function itself is not part
of the API.
"""
function sorting_sortspecs(sortspecs, colkeys::Union{Nothing,Keys} = nothing)
    sorting = map(ColumnSort, tuple(sortspecs...))
    sortkeys = sortkey.(sorting)
    @argcheck allunique(sortkeys) "Duplicate sort keys."
    colkeys ≡ nothing || @argcheck sortkeys ⊆ colkeys
    sorting
end

"""
$(SIGNATURES)

Calculate sorting when a table with `sorting` is merged with a table containing `otherkeys`,
which may replace columns.
"""
function merge_sorting(sorting::Sorting, otherkeys::Keys)
    firstinvalid = findfirst(s -> sortkey(s) ∈ otherkeys, sorting)
    firstinvalid ≡ nothing ? sorting : sorting[1:(firstinvalid-1)]
end

"""
$(SIGNATURES)

Calculate sorting when only `keep` keys are kept.
"""
select_sorting(sorting::Sorting, keep::Keys) = _select_sorting(keep, sorting...)

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

Compare `a` and `b`, which support the `getproperty` interface, with the given column
sorting.

*Internal*.
"""
cmp_sorting(sorting::Sorting, a, b) = _cmp_sorting(a, b, sorting...)

_cmp_sorting(a, b) = 0

function _cmp_sorting(a, b, cs::ColumnSort, rest...)
    r = cmp_columnsort(cs, a, b)
    r ≠ 0 && return r
    _cmp_sorting(a, b, rest...)
end
