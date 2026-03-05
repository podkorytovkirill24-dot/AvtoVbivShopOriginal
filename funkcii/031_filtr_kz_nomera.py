def filter_kz_numbers(numbers: List[str]) -> List[str]:
    accepted = []
    for n in numbers:
        if n.startswith("7") and 10 <= len(n) <= 11:
            accepted.append(n)
    return accepted
