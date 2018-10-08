module FunctionalTables

using ArgCheck: @argcheck
using DocStringExtensions: SIGNATURES, TYPEDEF
using Parameters: @unpack

import Base: length, IteratorSize, IteratorEltype, eltype, iterate

include("utilities.jl")
include("columns.jl")

end # module
