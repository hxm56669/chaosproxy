#include "chaosproxy/listener.h"

#include "chaosproxy/socket_ops.h"

#include <sys/epoll.h>

#include <cerrno>
#include <stdexcept>
#include <utility>

namespace chaosproxy {

Listener::Listener(
    EventLoop& loop,
    UniqueFd listen_fd,
    AcceptedCallback accepted_callback,
    ErrorCallback error_callback)
    : loop_(loop),
      listen_fd_(std::move(listen_fd)),
      accepted_callback_(std::move(accepted_callback)),
      error_callback_(std::move(error_callback)) {
    if (!listen_fd_.IsValid() ||
        !accepted_callback_ ||
        !error_callback_) {
        throw std::invalid_argument("invalid listener arguments");
    }

    token_ = loop_.Add(
        listen_fd_.Get(),
        EPOLLIN,
        [this](EventToken token, std::uint32_t events) {
            OnEvent(token, events);
        });
}

Listener::~Listener() noexcept {
    Stop();
}

int Listener::Fd() const noexcept {
    return listen_fd_.Get();
}

bool Listener::IsStarted() const noexcept {
    return token_ != 0;
}

void Listener::Stop() noexcept {
    if (token_ != 0) {
        (void)loop_.Remove(token_);
        token_ = 0;
    }
    listen_fd_.Reset();
}

void Listener::OnEvent(
    EventToken token,
    std::uint32_t events) noexcept {
    if (token == 0 || token != token_) {
        return;
    }

    if ((events & EPOLLERR) != 0U) {
        error_callback_(GetPendingSocketError(listen_fd_.Get()));
        return;
    }

    if ((events & (EPOLLHUP | EPOLLRDHUP)) != 0U) {
        error_callback_(ECONNABORTED);
        return;
    }

    if ((events & EPOLLIN) == 0U) {
        return;
    }

    for (;;) {
        AcceptResult result = TryAccept(listen_fd_.Get());
        if (result.status == AcceptStatus::kAccepted) {
            try {
                accepted_callback_(std::move(result.client_fd));
            } catch (...) {
                error_callback_(ECANCELED);
            }
            continue;
        }

        if (result.status == AcceptStatus::kError) {
            error_callback_(result.error_number);
        }
        return;
    }
}

}  // namespace chaosproxy
