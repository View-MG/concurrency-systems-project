#include <assert.h>
#include <stddef.h>

void count_http_errors(const unsigned int *, unsigned int, unsigned int *);

int main(void) {
    const unsigned int status[] = {200, 399, 400, 499, 500, 599, 600, 999};
    unsigned int errors = 123;
    assert(sizeof(unsigned int) == 4);
    count_http_errors(status, 8, &errors);
    assert(errors == 4);
    count_http_errors(status, 0, &errors);
    assert(errors == 0);
    count_http_errors(NULL, 0, &errors);
    assert(errors == 0);
    return 0;
}
