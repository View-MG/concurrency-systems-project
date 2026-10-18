/* Small, freestanding example for inspecting compiler-generated RV32I code.
 * The real Windows log analyzer remains separate; this is an ISA study.
 * status and error_count point to disjoint arrays/words of 32-bit unsigned int.
 */
void count_http_errors(const unsigned int *status, unsigned int length,
                       unsigned int *error_count) {
    unsigned int errors = 0;
    unsigned int index;
    for (index = 0; index < length; ++index) {
        if (status[index] >= 400u && status[index] <= 599u) {
            ++errors;
        }
    }
    *error_count = errors;
}
