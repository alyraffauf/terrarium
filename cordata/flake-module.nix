{ inputs, ... }:
{
  perSystem = { pkgs, self', ... }: {
    packages.cordata = pkgs.buildGoModule {
      pname = "cordata";
      version = "0.1.0";
      src = inputs.self;
      vendorHash = inputs.self.lib.vendorHash;
      subPackages = [ "cmd/cordata" ];
      env.CGO_ENABLED = 0;
      ldflags = [ "-s" "-w" ];
      checkPhase = ''
        runHook preCheck
        go test ./cordata/... ./palustre/...
        runHook postCheck
      '';
      meta = {
        description = "Maintain moderation lists of accounts that liked or reposted a post";
        mainProgram = "cordata";
        license = pkgs.lib.licenses.mit;
        platforms = pkgs.lib.platforms.unix;
      };
    };
    checks.cordata = self'.packages.cordata;
    devShells.cordata = self'.devShells.default;
  };
}
