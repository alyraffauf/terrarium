{ inputs, ... }:
{
  perSystem = { pkgs, self', ... }: {
    packages.helix = pkgs.buildGoModule {
      pname = "helix";
      version = "0.1.0";
      src = inputs.self;
      vendorHash = "sha256-yuhPn9ikgWr2drcyAhV1K6BKgmDBplLWWaTcJKUw13w=";
      subPackages = [ "cmd/helix" ];
      env.CGO_ENABLED = 0;
      ldflags = [ "-s" "-w" ];
      checkPhase = ''
        runHook preCheck
        go test ./helix/... ./palustre/...
        runHook postCheck
      '';
      meta = {
        description = "Maintain moderation lists of target accounts followers";
        mainProgram = "helix";
        license = pkgs.lib.licenses.mit;
        platforms = pkgs.lib.platforms.unix;
      };
    };
    checks.helix = self'.packages.helix;
    devShells.helix = self'.devShells.default;
  };
}
