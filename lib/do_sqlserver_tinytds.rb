$:.unshift File.dirname(__FILE__)

require 'data_objects'

require 'bigdecimal'
require 'date'
require 'base64'
#require 'do_sqlserver/do_sqlserver' if RUBY_PLATFORM =~ /java/
require 'do_sqlserver_tinytds/version'
# JDBC driver has transactions implementation in Java
require 'do_sqlserver_tinytds/transaction' #if RUBY_PLATFORM !~ /java/
require 'do_sqlserver_tinytds/tiny_tds_extension'
require 'do_sqlserver_tinytds/addressable_extension'
require 'pry'

  module DataObjects
    module SqlServer

      class Connection < DataObjects::Connection
        def method_missing(method, *args)
          if @connection.respond_to?(method)
            begin
              #args[0] - tiny tds accepts just one arg
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

        def debugger_on=(value)
          $debugger_on = value
        end

        def initialize uri
          $debugger_on = false
          # REVISIT: Allow uri.query to modify this connection's mode?
          #host = uri.host.blank? ? "localhost" : uri.host
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

          @options = {:username => uri.user,
                     :password => uri.password,
                     :port => port ,
                     :encoding => @encoding,
                     :timeout => 5000
                     }

          @options[:dataserver] = host

#          case
#            when uri.dataserver
#              options[:dataserver] = uri.dataserver
#            when host
#              options[:host] = host
#          end

          #connection_string = "DBI:ODBC:DRIVER=FreeTDS;SERVERNAME=sqlserver;DATABASE=#{path};"
          begin
            @connection = TinyTds::Client.new(@options).tap {|client| client.execute("SET ANSI_NULLS ON").do}
            #@connection = DBI.connect(connection_string, user, password)
          rescue Exception => e
            # Place to debug connection failures
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
      end

      class Command < DataObjects::Command
        # Theoretically, SCOPE_IDENTIY should be preferred, but there are cases where it returns a stale ID, and I don't know why.
        #IDENTITY_ROWCOUNT_QUERY = 'SELECT SCOPE_IDENTITY(), @@ROWCOUNT'
        #IDENTITY_ROWCOUNT_QUERY = 'SELECT @@IDENTITY, @@ROWCOUNT'
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
            #convert dynamic query into regular queries

            #debugger if @text.include?("tester") && @text.include?("non_existent_table")
            handle = @connection.raw_execute(@text , *args)
            handle.do
#          rescue DBI::DatabaseError => e
#            handle = @connection.raw.handle
#            handle.finish if handle && handle.respond_to?(:finish) && !handle.finished?
#            DataObjects::SqlServer.raise_db_error(e, @text, args)
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


          #handle.finish if handle && handle.respond_to?(:finish) && !handle.finished?

          # Get the inserted ID and the count of affected rows:
          inserted_id = @connection.execute("SELECT CAST(SCOPE_IDENTITY() AS bigint) AS Ident").each.first['Ident']
          row_count = handle.affected_rows#@connection.execute("SELECT @@ROWCOUNT AS AffectedRows").each.first['AffectedRows']

#          inserted_id, row_count = nil, nil
#
#          if (handle = @connection.raw_execute(IDENTITY_ROWCOUNT_QUERY))
#            #row1 = Array(Array(handle.each(:as => :array))[0])
#            row1 = handle.each.first
#            inserted_id, row_count = row1['Ident'] && row1['Ident'].to_i, row1['AffectedRows'].to_i
#            handle.cancel
#          end
          Result.new(self, row_count, inserted_id)
        end

        def execute_reader *args
          #debugger if args && args.first == "Buy this product now!"
          DataObjects::SqlServer.check_params @text, args
          massage_limit_and_offset args
          begin
            handle = @connection.raw_execute(@text, *args)

          rescue Exception => e

           handle.cancel if handle && handle.respond_to?(:cancel) && !@connection.sqlsent?
           raise
#          rescue
#
#            handle = @connection.raw.handle
#            handle.finish if handle && handle.respond_to?(:finish) && !handle.finished?
#            raise
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

            # Reverse the sort direction of each field in the ORDER BY:
            rev_order = order.split(/, */).map{ |f|
              f =~ /(.*) DESC *$/ ? $1 : f+" DESC"
            }*", "

            "SELECT TOP #{limit} * FROM (SELECT TOP #{offset+limit} #{what} ORDER BY #{rev_order}) ORDER BY #{order}"
          }
        end
      end

      class Result < DataObjects::Result

      end

      # REVISIT: There is no data type conversion happening here. That will make DataObjects sad.
      class Reader < DataObjects::Reader


        def initialize command, handle
          @command, @handle = command, handle
          return unless @handle

          #@fields = handle.column_names
          @fields = handle.fields

          # REVISIT: Prefetch results like AR's adapter does. ADO is a bit strange about handle lifetimes, don't move this until you can test it.
          @rows = []
          types = @command.types
          if types && types.size != @fields.size
            @handle.cancel if @handle && @handle.respond_to?(:cancel) #&& !@connection.sqlsent?
            #@handle.finish if @handle && @handle.respond_to?(:finish) && !@handle.finished?
            raise ArgumentError, "Field-count mismatch. Expected #{types.size} fields, but the query yielded #{@fields.size}"
          end


          @handle.each(:as => :array) do |row|
            field = -1
            @rows << row.map do |value|
              field += 1

              next value if !types && !value.is_a?(Time)
              debugger if $debugger_on

              if types.nil? && value.is_a?(Time)
                time_to_date_time(value)
              elsif value.nil? || types[field] == NilClass then
                nil
              elsif (t = types[field]) == Integer
                Integer(value)
              elsif value.is_a?(t)
                value
              elsif t == Float
                value && Float(value)
              elsif t == TrueClass
                value
              elsif t == String
                value.to_s
              elsif t == DateTime
                case
                  when value.is_a?(Time)
                    time_to_date_time(value)
                  when value.is_a?(String)
                    DateTime.parse(value)
                  else
                    DateTime.parse(value.to_s)
                end
              else
                #_value = value.respond_to?(:to_s) ? value.to_s : value
                begin
                  return_value = t.new(value)
                rescue Exception => e
                  if e.message[/can't convert \w+ into String/]
                    return_value = t.new(value.to_s)
                  end
                end
                return_value
                #(return_value.respond_to?(:to_s) && return_value.to_s) || return_value
              end
            end
          end
          @handle.cancel if @handle && @handle.respond_to?(:cancel)
          #@handle.finish if @handle && @handle.respond_to?(:finish) && !@handle.finished?
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
          #raise StandardError.new("First row has not been fetched") if @current_row < 0
          #raise StandardError.new("Last row has been processed") if @current_row >= @rows.size
          @rows[@current_row]
        end

        def fields
          @fields
        end

        def field_count
          @fields.size
        end

        # REVISIT: This is being deprecated
        def row_count
          @rows.size
        end

        private

        def time_to_date_time(value)
          DateTime.new(value.year, value.month, value.day,
                     value.hour, value.min, value.sec,
                     Rational(value.gmt_offset / 3600, 24))
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
          #puts "'#{cmd}' (#{args.map{|a| a.inspect}*", "}): #{e.to_str}"
          check_params(cmd, args)
        else
          e.errstr << " running '#{cmd}'"
          #puts "'#{cmd}' (#{args.map{|a| a.inspect}*", "}): #{e.to_str}"
          #debugger
        end
        raise DataObjects::SQLError.new(e.errstr)
      end

      def self.param_count cmd
        cmd.gsub(/'[^']*'/,'').gsub(/"[_|A-Z|a-z|0-9|?]+"/,'').scan(/\?/).size
      end

    end

  end

