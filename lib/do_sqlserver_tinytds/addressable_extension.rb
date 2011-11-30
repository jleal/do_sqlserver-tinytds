require "addressable/uri"

module Addressable
  class URI
    def self.parse(uri)
      # If we were given nil, return nil.
      return nil unless uri
      # If a URI object is passed, just return itself.
      return uri.dup if uri.kind_of?(self)

      # If a URI object of the Ruby standard library variety is passed,
      # convert it to a string, then parse the string.
      # We do the check this way because we don't want to accidentally
      # cause a missing constant exception to be thrown.
      if uri.class.name =~ /^URI\b/
        uri = uri.to_s
      end

      # Otherwise, convert to a String
      begin
        uri = uri.to_str
      rescue TypeError, NoMethodError
        raise TypeError, "Can't convert #{uri.class} into String."
      end if not uri.is_a? String

      # This Regexp supplied as an example in RFC 3986, and it works great.
      scan = uri.scan(URIREGEX)
      fragments = scan[0]
      scheme = fragments[1]
      authority = fragments[3]
      path = fragments[4]
      query = fragments[6]
      fragment = fragments[8]
      user = nil
      password = nil
      host = nil
      port = nil
      database = nil
      if authority != nil
        # The Regexp above doesn't split apart the authority.
        userinfo = authority[/^([^\[\]]*)@/, 1]
        if userinfo != nil
          user = userinfo.strip[/^([^:]*):?/, 1]
          password = userinfo.strip[/:(.*)$/, 1]
        end
        host = authority.gsub(/^([^\[\]]*)@/, EMPTYSTR).gsub(/:([^:@\[\]]*?)$/, EMPTYSTR)
        port = authority[/:(\d+).*$/ , 1]#authority[/:([^:@\[\]]*?)$/, 1]
        database = authority[/:\d+(.*)$/ , 1]
      end
      if port == EMPTYSTR
        port = nil
      end

      return Addressable::URI.new(
        :scheme => scheme,
        :user => user,
        :password => password,
        :host => host,
        :port => port,
        :path => path,
        :query => query,
        :fragment => fragment,
        :database => database
      )

    end
  end
end