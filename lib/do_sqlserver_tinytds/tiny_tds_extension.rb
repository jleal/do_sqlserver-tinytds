require 'tiny_tds'

module TinyTds
  class Client
    def raw_execute(text , *var_list)

      dynamic_sql = text.clone
      flat_sql = var_list.empty? ? dynamic_sql : process_questioned_sql(dynamic_sql , *var_list)
#      sql_type = classify_sql(dynamic_sql)
#
#      #Convert dynamic sql to flat sql statement
#      sqls = []
#      sqls = var_list.collect do |var|
#        debugger if $debugger_on
#        dynamic_sql.sub("?" , convert_type_to_s(var , sql_type))
#      end if !var_list.empty?

      #flat_sql = (!sqls.empty? && sqls.join(";\n")) || dynamic_sql
      debugger if $debugger_on
      execute(flat_sql)
    end

    def process_questioned_sql(sql , *vars)
      sql_type = classify_sql(sql)

      result = substitute_quoted_questions(sql)
      _sql = result[:sql]

      vars.each do |var|
        _sql.sub!("?" , convert_type_to_s(var , sql_type))
      end

      #place back strings in the sql that contained "?"
      result[:container].each do |k,v|
        _sql.gsub!(k , v)
      end

      _sql
    end

    def substitute_quoted_questions(sql)
      container = {}
      #collect strings in the sql that contains a question_mark
      qstrings = sql.scan(/"[^"]*"/).select{|s| s.include?("?")}

      #temporarily replace
      counter = 0
      qstrings.each do |qstring|
        key = "(((####{counter}###)))"
        sql.gsub!(qstring, key)

        container[key] = qstring
        counter += 1
      end

      {:sql => sql, :container => container}
    end

    def convert_type_to_s(arg , sql_type)

      case sql_type
        when :between
          case arg
            when Range , Array
              "#{sql_stringify_value(arg.first)} AND #{sql_stringify_value(arg.last)}"
            else
              raise "Type not found..."
          end
        when :in
          case arg
            when Range , Array
              " (#{arg.collect{|e| "#{sql_stringify_value(e)}"}.join(" , ")}) "
            else
              raise "Type not found..."
          end
        else
          case arg
            when Range , Array
              arg.collect{|e| "#{sql_stringify_value(e)}"}.join(" , ")
            else
              sql_stringify_value(arg)
          end
      end
    end

    def sql_stringify_value(value)
      case
        when value.is_a?(String)
          "N'#{value}'"
        when value.is_a?(Numeric)
          value.to_s
        when value.is_a?(NilClass)
          "NULL"
        when value.is_a?(DateTime)
          value.strftime("'%m/%d/%Y %I:%M:%S %p'")
        else
          "N'#{value.to_s}'"
      end
    end

    def classify_sql(sql)
      case
        when sql[/between.+\?/i] #var should be an array
          :between
        when sql[/\sin\s+\?/i]
          :in
      end
    end

  end

  class Error
    alias :to_str :to_s

    def errstr
      @errstr ||= []
      @errstr
    end


  end
end