# --- Build image
FROM ruby:3.0.1-alpine3.12 as builder
WORKDIR /app

# bundle install deps
RUN apk add --update ca-certificates git build-base openssl-dev
RUN gem install bundler -v '>= 2'

# bundle install
COPY Gemfile* ./
RUN bundle

# --- Runtime image
FROM ruby:3.0.1-alpine3.12
WORKDIR /app

COPY --from=builder /usr/local/bundle /usr/local/bundle
RUN apk --update upgrade && apk add --no-cache ca-certificates coreutils rsync

COPY . .
RUN addgroup -g 1000 -S app \
  && adduser -u 1000 -S app -G app \
  && chown -R app: .

USER app
ENTRYPOINT ["bundle", "exec", "ruby", "main.rb", "prune"]
