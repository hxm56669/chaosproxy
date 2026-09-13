#include "phototask/runtime_status.h"

#include <CLI/CLI.hpp>
#include <drogon/drogon.h>

#include <iostream>

int main(int argc, char** argv) {
  CLI::App app{"PhotoTask HTTP runtime"};
  bool health = false;
  std::string listen = "0.0.0.0:8080";
  app.add_flag("--health", health, "Print a local health response and exit");
  app.add_option("--listen", listen, "listen address host:port");
  CLI11_PARSE(app, argc, argv);
  phototask::RuntimeStatus status;
  status.SetReady(true);
  if (health) {
    std::cout << status.Livez();
    return 0;
  }
  const auto separator = listen.rfind(':');
  if (separator == std::string::npos) {
    std::cerr << "--listen must be host:port\n";
    return 2;
  }
  const std::string host = listen.substr(0, separator);
  const auto port = static_cast<unsigned short>(std::stoul(listen.substr(separator + 1)));
  drogon::app().registerHandler("/livez", [&status](const drogon::HttpRequestPtr&,
                                                     std::function<void(const drogon::HttpResponsePtr&)>&& callback) {
    auto response = drogon::HttpResponse::newHttpResponse();
    response->setBody(status.Livez());
    callback(response);
  }, {drogon::Get}).registerHandler("/readyz", [&status](const drogon::HttpRequestPtr&,
                                                          std::function<void(const drogon::HttpResponsePtr&)>&& callback) {
    auto response = drogon::HttpResponse::newHttpResponse();
    response->setBody(status.Readyz());
    callback(response);
  }, {drogon::Get}).registerHandler("/metrics", [&status](const drogon::HttpRequestPtr&,
                                                           std::function<void(const drogon::HttpResponsePtr&)>&& callback) {
    auto response = drogon::HttpResponse::newHttpResponse();
    response->setBody(status.Metrics());
    callback(response);
  }, {drogon::Get}).addListener(host, port).run();
  return 0;
}
