$:.unshift File.dirname(__FILE__)

require 'data_objects'
require 'bigdecimal'
require 'date'
require 'base64'
require 'do_sqlserver_tinytds/version'
require 'do_sqlserver_tinytds/transaction'
require 'do_sqlserver_tinytds/tiny_tds_extension'
require 'do_sqlserver_tinytds/addressable_extension'


  module DataObjects
    module SqlServer

      class Connection < DataObjects::Connection
        def method_missing(method, *args)
          if @connection.respond_to?(method)
            begin
              @connection.send(method , *args)
            rescue TinyTds::Error => te
              case te.db_error_number
                when 20019
                  #Got a pending transcation there, lets make it let go
                  @connection.close
                  @connection = TinyTds::Client.new(@options).tap {|client| client.execute("SET ANSI_NULLS ON").do}
                  #give it another try
                  @connection.send(method, *args)
                else
                  raise te
              end
            end
          end
        end

        def initialize uri
          host = uri.host
          user = uri.user || "sa"
          password = uri.password || ""
          path = uri.path.sub(%r{^/*}, '')
          port = uri.port || "1433"

          begin
            _encoding = uri.query && uri.query["encoding"] || "UTF-8"
            @encoding = Encoding.find(_encoding) ? _encoding : "UTF-8"
          rescue
            @encoding = "UTF-8"
          end

          @options = {:username => user,
                     :password => password,
                     :port => port ,
                     :encoding => @encoding,
                     :timeout => 5000,
                     :dataserver => host,
                     :database => path
                     }

          @options[:dataserver] = host

          begin
            @connection = TinyTds::Client.new(@options).tap {|client| client.execute("SET ANSI_NULLS ON").do}
            #@connection = DBI.connect(connection_string, user, password)
          rescue Exception => e
            raise
          end

          set_date_format = create_command("SET DATEFORMAT YMD").execute_non_query
          options_reader = create_command("DBCC USEROPTIONS").execute_reader
          while options_reader.next!
            key, value = *options_reader.values
            value = options_reader.values
            case key
            when "textsize"                     # "64512"
            when "language"                     # "us_english", "select * from master..syslanguages" for info
            when "dateformat"                   # "ymd"
            when "datefirst"                    # "7" = Sunday, first day of the week, change with "SET DATEFIRST"
            when "quoted_identifier"            # "SET"
            when "ansi_null_dflt_on"            # "SET"
            when "ansi_defaults"                # "SET"
            when "ansi_warnings"                # "SET"
            when "ansi_padding"                 # "SET"
            when "ansi_nulls"                   # "SET"
            when "concat_null_yields_null"      # "SET"
            else
            end
          end
        end

        def using_socket?
          # This might be an unnecessary feature dragged from the mysql driver
          raise "Not yet implemented"
        end

        def character_set
          @encoding
        end

        def dispose
          return false if @connection.closed?
          @connection.close
          true
        rescue
          false
        end

        def raw
          @connection
        end

        :private

        #debugger_on - used only for development when debugging things
        def debugger_on=(value)
          $debugger_on = value
        end
      end

      class Command < DataObjects::Command
        IDENTITY_ROWCOUNT_QUERY = 'SELECT CAST(SCOPE_IDENTITY() AS bigint) AS Ident, @@ROWCOUNT AS AffectedRows'

        attr_reader :types

        def debugger_on(value)
          $debugger_on = value
        end

        def set_types *t
          @types = t.flatten
        end

        def execute_non_query *args
          DataObjects::SqlServer.check_params @text, args

          begin
            handle = @connection.raw_execute(@text , *args)
            handle.do
          rescue TinyTds::Error => te

            handle.cancel if handle && handle.respond_to?(:cancel) && !@connection.sqlsent?

            DataObjects::SqlServer.raise_db_error(te, @text, args)
          rescue RuntimeError => re
            case re.message
              when "closed connection"
                raise DataObjects::ConnectionError.new(re.message)
            end
          rescue Exception => e
            raise e
          end

          # Get the inserted ID and the count of affected rows:
          inserted_id = @connection.execute("SELECT CAST(SCOPE_IDENTITY() AS bigint) AS Ident").each.first['Ident']
          row_count = handle.affected_rows
          Result.new(self, row_count, inserted_id)
        end

        def execute_reader *args
          DataObjects::SqlServer.check_params @text, args
          massage_limit_and_offset args
          begin
            handle = @connection.raw_execute(@text, *args)

          rescue Exception => e

           handle.cancel if handle && handle.respond_to?(:cancel) && !@connection.sqlsent?
           raise
          end

          Reader.new(self, handle)
        end

      private
        def massage_limit_and_offset args
          @text.sub!(%r{SELECT (.*) ORDER BY (.*) LIMIT ([?0-9]*)( OFFSET ([?0-9]*))?}) {
            what, order, limit, offset = $1, $2, $3, $5

            # LIMIT and OFFSET will probably be set by args. We need exact values, so must
            # do substitution here, and remove those args from the array. This is made easier
            # because LIMIT and OFFSET are always the last args in the array.
            offset = args.pop if offset == '?'
            limit = args.pop if limit == '?'
            offset = offset.to_i
            limit = limit.to_i

            #Reverse the sort direction of each field in the ORDER BY:
            rev_order = order.split(/, */).map{ |f|
              f =~ /(.*) DESC *$/ ? $1 : f+" DESC"
            }*", "

            "SELECT TOP #{limit} * FROM (SELECT TOP #{offset+limit} #{what} ORDER BY #{rev_order}) ORDER BY #{order}"
          }
        end
      end

      class Result < DataObjects::Result
      end

      class Reader < DataObjects::Reader
        def initialize command, handle
          @command, @handle = command, handle
          return unless @handle

          @fields = handle.fields

          @rows = []
          types = @command.types
          if types && types.size != @fields.size
            @handle.cancel if @handle && @handle.respond_to?(:cancel)
            raise ArgumentError, "Field-count mismatch. Expected #{types.size} fields, but the query yielded #{@fields.size}"
          end


          @handle.each(:as => :array) do |row|
            field = -1
            @rows << row.map do |value|
              field += 1

              next value if !types && !value.is_a?(Time)

              field_type = types && types[field]
              value_class = value.class

              r_value = case
                when field_type == NilClass || value.nil?
                  nil
                when field_type.nil? && value.is_a?(Time)
                  #sql small dates have zeros for hr, min, sec
                  # and needs to be cast as Date, else cast as DateTime
                  if (value.hour + value.min + value.sec) == 0
                    time_to_date(value)
                  else
                    time_to_date_time(value)
                  end
                when value.is_a?(field_type) || value_class.kind_of?(field_type) || field_type == TrueClass
                  value
                when field_type == Integer
                  Integer(value)
                when field_type == Float
                  Float(value)
                when field_type == String
                  raise "Value '#{value.inspect}' does not respond to #to_s" unless value.respond_to?(:to_s)
                  value.to_s
                when field_type == DateTime
                  case
                    when value.is_a?(Time)
                      time_to_date_time(value)
                    when value.is_a?(String)
                      DateTime.parse(value)
                    else
                      DateTime.parse(value.to_s)
                  end
                when field_type == Date
                  case
                    when value.is_a?(Time) || value.is_a?(DateTime)
                      Date.parse(value.strftime('%Y/%m/%d'))
                    else
                      Date.parse(value)
                  end
                when field_type == Time
                  Time.parse(value)
                when field_type == BigDecimal
                  BigDecimal.new(value.to_s)
                else
                  if value.respond_to?(:to_s)
                    value.to_s
                  else
                    value
                  end
              end

              r_value

            end
          end
          @handle.cancel if @handle && @handle.respond_to?(:cancel)
          @current_row = -1
        end

        def close
          if @handle
            @handle.finish if  @handle.respond_to?(:finish) && !@handle.finished?
            @handle = nil
            true
          else
            false
          end
        end

        def next!
          (@current_row += 1) < @rows.size
        end

        def values
          raise DataObjects::DataError.new("First row has not been fetched") if @current_row < 0
          raise DataObjects::DataError.new("Last row has been processed") if @current_row >= @rows.size
          @rows[@current_row]
        end

        def fields
          @fields
        end

        def field_count
          @fields.size
        end

        def row_count
          @rows.size
        end

        private

        def time_to_date_time(value)
          DateTime.new(value.year, value.month, value.day,
                     value.hour, value.min, value.sec,
                     Rational(value.gmt_offset / 3600, 24))
        end

        def time_to_date(value)
          Date.parse(time_to_date_time(value).strftime('%Y/%m/%d'))
        end
      end

    private

      def self.check_params cmd, args
        actual = args.size
        expected = param_count(cmd)
        raise ArgumentError.new("Binding mismatch: #{actual} for #{expected}") if actual != expected
      end

      def self.raise_db_error(e, cmd, args)
        msg = e.to_str
        case msg
        when /Too much parameters/, /No data found/
          check_params(cmd, args)
        else
          e.errstr << " running '#{cmd}'"
        end
        raise DataObjects::SQLError.new(e.errstr)
      end

      def self.param_count cmd
        cmd.gsub(/'[^']*'/,'').gsub(/"[_|A-Z|a-z|0-9|?]+"/,'').scan(/\?/).size
      end

    end

  end

