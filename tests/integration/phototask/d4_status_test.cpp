#include "phototask/runtime_status.h"

#include <gtest/gtest.h>

TEST(D4StatusTest, HealthAndMetricsAreStableAndCheap) {
  phototask::RuntimeStatus status;
  EXPECT_EQ(status.Livez(), "ok\n");
  EXPECT_EQ(status.Readyz(), "not ready\n");
  status.SetReady(true);
  status.CountRequest();
  EXPECT_EQ(status.Readyz(), "ready\n");
  EXPECT_NE(status.Metrics().find("phototask_requests_total 1"), std::string::npos);
}
