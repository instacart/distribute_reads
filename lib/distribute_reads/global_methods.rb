module DistributeReads
  module GlobalMethods

    SUPPRESS_REPORTING = !!ENV["DISTRIBUTE_READS_NO_REPORT_LAG"]
    private_constant :SUPPRESS_REPORTING

    def distribute_reads(**options)
      raise ArgumentError, "Missing block" unless block_given?

      unknown_keywords = options.keys - [:failover, :lag_failover, :lag_on, :max_lag, :primary, :replica]
      raise ArgumentError, "Unknown keywords: #{unknown_keywords.join(", ")}" if unknown_keywords.any?

      options = DistributeReads.default_options.merge(options)

      previous_value = Thread.current[:distribute_reads]
      begin
        Thread.current[:distribute_reads] = {
          failover: options[:failover],
          primary: options[:primary],
          replica: options[:replica]
        }

        # TODO ensure same connection is used to test lag and execute queries
        max_lag = options[:max_lag]
        if max_lag && !options[:primary]
          Array(options[:lag_on] || [ActiveRecord::Base]).each do |base_model|
            if (lag = DistributeReads.lag(connection: base_model.connection)) > max_lag
              message = "Replica lag #{lag} over app requested #{max_lag} seconds on #{base_model.name} connection."

              if options[:lag_failover]
                # TODO possibly per connection
                Thread.current[:distribute_reads][:primary] = true
                Thread.current[:distribute_reads][:replica] = false
                DistributeReads.log "#{message} Falling back to master pool for all databases."
                report_lag("distribute_reads.lag_failover", lag, base_model.name)
                break
              else
                report_lag("distribute_reads.lag_error", lag, base_model.name)
                raise DistributeReads::TooMuchLag, message
              end
            end
          end
        end

        value = yield
        DistributeReads.log "Call `to_a` inside block to execute query on replica" if value.is_a?(ActiveRecord::Relation) && !previous_value
        value
      ensure
        Thread.current[:distribute_reads] = previous_value
      end
    end

    def report_lag(message, lag, model_name)
      # TODO: distribute_reads gem shouldn't depend on ICMetrics directly. This is quick fix due to load (3/21/2020)
      ICMetrics.event(message, lag, model: model_name) unless SUPPRESS_REPORTING
    end
  end
end
