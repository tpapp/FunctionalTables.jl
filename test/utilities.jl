# equality operator for unit tests
≅(a, b) = a == b
≅(::Missing, ::Missing) = true
≅(a::AbstractVector, b::AbstractVector) = length(a) == length(b) && all(a .≅ b)

function randvector_fs(; range = 1:1000)
    ranl() = rand() < 0.5 ? 1 : rand(range)
    [() -> fill(missing, ranl()),
     () -> fill(rand(Int8.(1:100)), ranl()),
     () -> fill(rand(Float64.(1:100)), ranl())]
end

function randvector(N, fs = randvector_fs())
    m = length(fs)
    v = Any[]
    for _ in 1:N
        v = vcat(v, fs[rand(1:m)]())
    end
    collect(v)
end
