#include <iostream>
#include <vector>
#include "bar.hpp"

template <typename T>
T sum_all(const std::vector<T>& xs) {
    T total{};
    for (const auto& x : xs) total += x;
    return total;
}

int main() {
    std::vector<int> xs{1, 2, 3, 4, 5};
    std::cout << "foo: " << bar::greeting() << " sum=" << sum_all(xs) << "\n";
    return 0;
}
