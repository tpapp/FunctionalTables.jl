"""
$(SIGNATURES)

Test if a collection of element type `T` can contain a new element of type `S`.
"""
@inline cancontain(T, S) = S <: T || T ≡ promote_type(S, T)

"""
$(SIGNATURES)

Append `elt` to `v`, allocating a new vector and copying the contents.

Type of new collection is calculated using `promote_type`.
"""
function append1(v::Vector{T}, elt::S) where {T,S}
    U = promote_type(T, S)
    w = Vector{U}(undef, length(v) + 1)
    copyto!(w, v)
    w[end] = elt
    w
end
